################################################################################################
# Author             :           Sabyasachi Das
# Project            :           GPS-PathFinder
# Function           :           Classification Engine
# Description        :           Reads JSON data from conform zone and uploads to ElsaticSearch
################################################################################################

#########################Imports################################################################

import boto3, requests, sys, time, datetime, os
import simplejson as json
from boto3 import session
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from requests_aws_sign import AWSV4Sign
import pandas as pd
from datetime import datetime

fmt = '%Y-%m-%d %H:%M:%S'

#########################Set default system encoding to UTF-8###################################

reload(sys)
sys.setdefaultencoding('utf8')

#########################Define resource clients and resource names#############################

Notes = 'instanceType:xlarge \n Added max_chunk_bytes=524288000'
mappingfile = 'mapping.json'
env = sys.argv[1]
host = sys.argv[2]
indexname = sys.argv[3]
filename = sys.argv[4]

NoofInstances = '4'
NoofShards = '12'
ChunkSize = 5000
ThreadCount = 4
d1 = datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), fmt)
d1_ts = time.mktime(d1.timetuple())

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
scheduler_table = dynamodb.Table('v486121-GPS-PathFinder-Scheduler-Table-' + env)
validation_table = dynamodb.Table('v486121-GPS-PathFinder-Validation-Rules-' + env)

sns_client = boto3.client('sns', region_name='us-east-1')
TopicArn = 'arn:aws:sns:us-east-1:820784505615:v486121-GPS-PathFinder-SNS-DP-Notification-Topic-' + env

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
    print('Creating elasticsearch instance failed because : ' + str(err))


#########################Define method to upload data to elasticsearch###########################

def upload_generator(filename, index, doc_type, id, src, tbl, classifier):
    print ('----------------------Uploading ' + src + ' data----------------------')
    print ('Source file : ' + filename)
    try:
        df = pd.read_csv(filename, engine='python', parse_dates=True, delimiter='|', na_values='null', chunksize=100000,
                         header=0, error_bad_lines=False)
        for i in df:
            x = i.fillna(value='').to_json(orient='records')
            config = json.loads(x)
            for i in config:
                i.update({'Source': src, 'Classifier': i[classifier]})
                yield {
                    '_index': index,
                    '_id': id,
                    '_type': doc_type,
                    '_source': {
                        'data': i
                    }
                }
                id += 1
    except:
        e = sys.exc_info()[1]
        print('Generator failed because : ' + str(e) + ' :File was not found!')
        sns_client.publish(
            TopicArn=TopicArn,
            Message='Upload for ' + str(tableList) + ' failed because ' + str(e),
            Subject="Performance-" + indexname + env + "SAP" + "-FAILED"
        )
        exit(0)


#######################################Bulk upload method########################################

def parallel_bulkUpload(filename, indexname, doc_type, count, tbl):
    try:
        for k, v in id.iteritems():
            action = upload_generator(filename, indexname, doc_type, int(count), src, tbl, v)
            for success, info in helpers.parallel_bulk(es, action, chunk_size=5000, thread_count=4):
                if not success: print('Doc failed because: ', info)
        time.sleep(1)
        es.indices.refresh(index=indexname)
    except:
        e = sys.exc_info()[1]
        print('Uploading the data file failed because : ' + str(e))
        sns_client.publish(
            TopicArn=TopicArn,
            Message='Upload for ' + str(tableList) + ' failed : ' + str(e) + str(datetime.now()),
            Subject="Performance-" + " " + indexname + " " + env + " " + filename + " " + "SAP" + "-FAILED"
        )
        print ('********************FAILED********************')
        exit(0)


#####################################Create list of tables to be scanned#############################

response = scheduler_table.scan()
schedule_data = eval(json.dumps(response))
response = validation_table.scan()
validate_data = eval(json.dumps(response))

for i in schedule_data['Items']:
    if i['run_status'] == 'Ready':
        for j in validate_data['Items']:
            if j['Source'] == i['Source_Type']:
                if j.has_key('TableList'):
                    tableList = j['TableList']

print ('The List of Tables to be processed are : ' + str(tableList))
if len(tableList) == 0:
    print ('There is no data to be processed currently!!!')
    print ('Exiting!!!')
    exit(0)
fullname = sys.argv[6]

for i in schedule_data['Items']:
    if i['DestinationPath'] == '/'.join(fullname.split('/')[-5:]):
        doc_type = i['Doc Type']
        id = i['ClassifierMap']
        src = i['Source_Type']
        tbl = i['Table_Name']

# print (filename, indexname, doc_type, count, tbl)

#########################Loop through the table list and run upload method#######################

sns_client.publish(
    TopicArn=TopicArn,
    Message='Upload for ' + str(tableList) + ' has started at ' + str(datetime.now()),
    Subject="Performance-" + " " + indexname + " " + env + " " + filename + " " + "SAP" + "-START"
)

count = sys.argv[5]

parallel_bulkUpload(filename, indexname, doc_type, count, tbl)

d2 = datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), fmt)
d2_ts = time.mktime(d2.timetuple())
timetaken = int(d2_ts - d1_ts)

print 'indexname' + indexname
print 'TimeTaken:::::::::::::::::' + str(timetaken) + 'sec'

sns_client.publish(
    TopicArn=TopicArn,
    Message='Notes:' + str(Notes) + '\n Time Taken:' + str(timetaken) + 'sec;\n env:' + str(env) + '\n host: ' + str(
        host) + '\n indexname:' + str(indexname) + '\n filename:' + str(filename) + '\n No of Instances:' + str(
        NoofInstances) + '\n NoofShards' + str(NoofShards) + '\n ChunkSize' + str(ChunkSize) + '\n ThreadCount' + str(
        ThreadCount) + '\n Finished at :' + str(datetime.now()),
    Subject="Performance-" + " " + indexname + " " + env + " " + filename + " " + "SAP" + "-END"
)

print ('********************THE END********************')

############################################END##################################################
