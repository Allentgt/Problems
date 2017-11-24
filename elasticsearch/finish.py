################################################################################################
# Author             :           Sabyasachi Das
# Project            :           GPS-PathFinder
# Function           :           Classification Engine
# Description        :           Reads JSON data from conform zone and uploads to ElsaticSearch
################################################################################################

#########################Imports################################################################

import boto3, requests, sys, os
import simplejson as json
from boto3 import session
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from requests_aws_sign import AWSV4Sign

reload(sys)
sys.setdefaultencoding('utf8')

#########################Define resource clients and resource names#############################

env = sys.argv[1]
host = sys.argv[2]
indexname = sys.argv[3]
wc = sys.argv[4]
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

scheduler_table = dynamodb.Table('v486121-GPS-PathFinder-Scheduler-Table-' + env)
validation_table = dynamodb.Table('v486121-GPS-PathFinder-Validation-Rules-' + env)

response = scheduler_table.scan()
schedule_data = eval(json.dumps(response))
response = validation_table.scan()
validate_data = eval(json.dumps(response))

#########################Create elasticsearch session instance###################################

session = session.Session()
credentials = session.get_credentials()
region = 'us-east-1'
service = 'es'
auth = AWSV4Sign(credentials, region, service)
try:
    es = Elasticsearch(host=host,
                   port=443,
                   connection_class=RequestsHttpConnection,
                   http_auth=auth,
                   use_ssl=True,
                   verify_ssl=True,
                   timeout=30,
                   max_retries=10,
                   retry_on_timeout=True
                    )
except Exception, err:
    print('Creating elasticsearch instance failed because : '+str(err))


sns_client = boto3.client('sns', region_name='us-east-1')
TopicArn = 'arn:aws:sns:us-east-1:820784505615:v486121-GPS-PathFinder-SNS-DP-Notification-Topic-' + env

if indexname == 'datastore':
    altindex = 'datastore_bkp'
else:
    altindex = 'datastore'

if int(wc) == int(es.count(index=indexname)['count']):
    print ('The load was successful!!!')
    es.indices.delete(index=altindex)
    es.indices.put_alias(index=indexname, name='datanode')
    for i in schedule_data['Items']:
        if i['run_status'] == 'Ready':
            scheduler_table.update_item(
                Key={
                "Source_Type": i['Source_Type'],
                "Table_Name": i['Table_Name']
                },
                UpdateExpression="SET run_status = :r",
                ExpressionAttributeValues={
                    ':r': 'Completed'
                }
            )

    response = sns_client.publish(
                TopicArn = TopicArn,
                Message = 'The upload completed',
                Subject = "GPS - PathFinder - Elasticsearch - " + indexname + " " + str(wc) + " -SUCCESSFUL")
else:
    print ('The load failed!!!')
    response = sns_client.publish(
                TopicArn = TopicArn,
                Message = 'The upload failed',
                Subject = "GPS - PathFinder - Elasticsearch - " + indexname + " " + str(wc) + " -FAIL"
        )
