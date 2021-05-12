#     Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#     Licensed under the Apache License, Version 2.0 (the "License").
#     You may not use this file except in compliance with the License.
#     A copy of the License is located at
#
#         https://aws.amazon.com/apache-2-0/
#
#     or in the "license" file accompanying this file. This file is distributed
#     on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
#     express or implied. See the License for the specific language governing
#     permissions and limitations under the License.

import requests
from datetime import datetime, timedelta
import getopt, sys
import urllib3
import boto3
import json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Usage
usageInfo = """Usage:
This scripts checks if a notebook is idle for X seconds if it does, it'll stop the notebook:
python autostop.py --time <time_in_seconds> [--port <jupyter_port>] [--ignore-connections]
Type "python autostop.py -h" for available options.
"""
# Help info
helpInfo = """-t, --time
    Auto stop time in seconds
-p, --port
    jupyter port
-c --ignore-connections
    Stop notebook once idle, ignore connected users
-h, --help
    Help information
"""

# Read in command-line parameters
idle = True
port = '8443'
ignore_connections = False
try:
    opts, args = getopt.getopt(sys.argv[1:], "ht:p:c", ["help","time=","port=","ignore-connections"])
    if len(opts) == 0:
        raise getopt.GetoptError("No input parameters!")
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(helpInfo)
            exit(0)
        if opt in ("-t", "--time"):
            time = int(arg)
        if opt in ("-p", "--port"):
            port = str(arg)
        if opt in ("-c", "--ignore-connections"):
            ignore_connections = True
except getopt.GetoptError:
    print(usageInfo)
    exit(1)

# Missing configuration notification
missingConfiguration = False
if not time:
    print("Missing '-t' or '--time'")
    missingConfiguration = True
if missingConfiguration:
    exit(2)


def is_idle(last_activity):
    last_activity = datetime.strptime(last_activity,"%Y-%m-%dT%H:%M:%S.%fz")
    if (datetime.now() - last_activity).total_seconds() > time:
        return True
    else:
        return False


def get_notebook_name():
    log_path = '/opt/ml/metadata/resource-metadata.json'
    with open(log_path, 'r') as logs:
        _logs = json.load(logs)
    return _logs['ResourceName']

def last_kernel_execution_activity(kernel):
    if kernel['execution_state'] != 'idle':
        return datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fz")
    return kernel['last_activity'];

def last_kernel_connection_activity(kernel):
    if kernel['connections'] > 0:
        return datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fz")
    return kernel['last_activity']


last_active_time = datetime.now() - timedelta(days=3*365)
response = requests.get('https://localhost:'+port+'/api/sessions', verify=False)
notebooks = response.json()
print("Session Data:", notebooks)
activities = []

execution_activities = [('execution', last_kernel_execution_activity(n['kernel'])) for n in notebooks]
activities.extend(execution_activities)

connection_activities = [('connection', last_kernel_connection_activity(n['kernel'])) for n in notebooks if not ignore_connections]
activities.extend(connection_activities)

client = boto3.client('sagemaker')
uptime = client.describe_notebook_instance(NotebookInstanceName=get_notebook_name())['LastModifiedTime']
activities.append(('instance configuration', uptime.strftime("%Y-%m-%dT%H:%M:%S.%fz")))

resource, last_active_time = max(activities, key=lambda x: x[1])

print(f"Last activity resource={resource} time={last_active_time}")

# ==================== Quota Usage Handler =======================
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('devhub')

def update_session(timestamp, stage, username, month):
    if stage == 0:
        key = 'startSess'
    else:
        key = 'endSess'

    table.update_item(
        Key={
            'username': username,
            'month': month
        },
        UpdateExpression=f"SET {key} = list_append({key}, :i)",
        ExpressionAttributeValues={
            ':i': [timestamp],
        },
        ReturnValues="UPDATED_NEW"
    )

def update_value(key, value, username, month):
    table.update_item(
        Key={
            'username': username,
            'month': month
        },
        UpdateExpression=f"SET {key} = :i",
        ExpressionAttributeValues={
            ':i': value,
        },
        ReturnValues="UPDATED_NEW"
    )

def sec2str(seconds):
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return f'{int(hours)}h {int(minutes)}m'

def str2sec(string):
    hour_str, min_str = string.split()
    hours, minutes = int(hour_str[:-1]), int(min_str[:-1])
    minutes = minutes + (hours * 60)
    seconds = minutes * 60
    return seconds

def to_datetime(time_str):
    return datetime.strptime(time_str, "%Y-%m-%d %H:%M")

username = get_notebook_name()[3:]
curr_month = datetime.utcnow().strftime('%Y-%m')
data = table.get_item(Key={'username': username, 'month': curr_month})['Item']
curr_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
quota = timedelta(hours=int(data['quota'])).total_seconds()
is_quota = False

# On start of the notebook
if data['lastPing'] == 'inactive':
    update_session(curr_time, 0, username, curr_month)
    update_value('lastPing', curr_time, username, curr_month)
    print('Notebook Starting...Sent the first Ping!')
# On cron ping
else:
    add_usage = to_datetime(curr_time) - to_datetime(data['lastPing'])
    new_usage = str2sec(data['used']) + add_usage.total_seconds()
    print('Ping...New Usage', new_usage)
    if new_usage > quota:
        print('Quota Exceeded')
        update_value('used', sec2str(quota), username, curr_month)
        is_quota = True
    else:
        update_value('used', sec2str(new_usage), username, curr_month)
        update_value('lastPing', curr_time, username, curr_month)


if is_idle(last_active_time) or is_quota:
    print("Shutting down the instance")

    update_session(curr_time, 1, username, curr_month)
    update_value('lastPing', 'inactive', username, curr_month)

    client = boto3.client('sagemaker')
    client.stop_notebook_instance(
        NotebookInstanceName=get_notebook_name()
    )
