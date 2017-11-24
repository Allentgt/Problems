################################################################################################
# Author             :           Sabyasachi Das
# Project            :           GPS-PathFinder
# Function           :           Classification Engine
# Description        :           Reads mapping data from S3 and creates index in ElsaticSearch
################################################################################################

#########################Imports################################################################

import boto3, requests, sys, os
import simplejson as json
from boto3 import session
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from requests_aws_sign import AWSV4Sign

#########################Set default system encoding to UTF-8###################################

reload(sys)
sys.setdefaultencoding('utf8')

#########################Define resource clients and resource names#############################

mappingfile='mapping.json'
env = sys.argv[1]
host = sys.argv[2]
indexname = sys.argv[3]
s3_client = boto3.client('s3')

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

####################Define method to create column mapping in elasticsearch######################

def column_mapping(index):
    try:
        res = s3_client.get_object(Bucket='temp-devops', Key='GPS-PathFinder/'+env+'/Setupfiles/mappings/'+mappingfile)
        config = res['Body'].read().decode('utf-8')
        config = json.loads(config)
        es.indices.create(index=index, body=config)
    except Exception, err:
        print('Loading the mapping file failed because : '+str(err))

if not es.indices.exists(index=indexname):
    column_mapping(indexname)
if not es.indices.exists(index=indexname + '_bkp'):
    column_mapping(indexname + '_bkp')

if es.indices.exists_alias(index=indexname, name='datanode'):
    print (indexname + '_bkp')
elif es.indices.exists_alias(index=indexname + '_bkp', name='datanode'):
    print (indexname)
else:
    print (indexname)
        
