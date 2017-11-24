################################################################################################
# Author             :           Sabyasachi Das
# Project            :           GPS-PathFinder
# Function           :           Classification Engine
# Description        :           Reads JSON data from conform zone and uploads to ElsaticSearch
################################################################################################

#########################Imports################################################################

import boto3, sys
import simplejson as json

#########################Define resource clients and resource names#############################

env = sys.argv[1]

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
scheduler_table = dynamodb.Table('v486121-GPS-PathFinder-Scheduler-Table-' + env)
validation_table = dynamodb.Table('v486121-GPS-PathFinder-Validation-Rules-' + env)

response = scheduler_table.scan()
schedule_data = eval(json.dumps(response))
response = validation_table.scan()
validate_data = eval(json.dumps(response))

tableList = []

for i in schedule_data['Items']:
    if i['run_status'] == 'Ready':
        for j in validate_data['Items']:
            if j['Source'] == i['Source_Type']:
                if j.has_key('TableList'):
                    tableList = j['TableList']

if len(tableList)==0:
    print ('There is no data to be processed currently!!!' )
    print ('Exiting!!!')
    exit(0)

filename = []

for i in schedule_data['Items']:
    for j in tableList:
        if i['Table_Name'] == j:
            filename.append('s3://bms-pathfinder-' + env + '/' + i['DestinationPath'])
for i in filename:
    print (i)
	
