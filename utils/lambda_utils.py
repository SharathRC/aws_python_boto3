import boto3
import json
from pprint import pprint
from loguru import logger

from s3_utils import list_all_buckets, add_object_to_bucket
from dynamodb_utils import (
    create_db_table, 
    insert_batch_data, 
    get_batch_items, 
    query_table,
    update_item_in_table,
    delete_item_in_table,
)


def create_lambda_role():
    iam = boto3.client('iam')

    role_policy = {

        "Version":"2012-10-17",
        "Statement":[
            {
                "Sid":"",
                "Effect":"Allow",
                "Principal":{
                    "Service":"lambda.amazonaws.com"
                },
                "Action":"sts:AssumeRole"
            }

        ]


    }

    response = iam.create_role(
        RoleName='PyLambdaBasicExecution',
        AssumeRolePolicyDocument=json.dumps(role_policy)

    )

    print(response)

def create_lambda_function(lambda_function_zip:str='lambda.zip'):
    iam_client = boto3.client('iam')
    lambda_client = boto3.client('lambda')


    with open(lambda_function_zip, 'rb') as f:
        zipped_code = f.read()


    role = iam_client.get_role(RoleName='PyLambdaBasicExecution')

    response = lambda_client.create_function(
        FunctionName='helloWorldLambda',
        Runtime='python3.9',
        Role=role['Role']['Arn'],
        Handler='lambda_function.lambda_handler',
        Code=dict(ZipFile=zipped_code),
        Timeout=300,
    )

    logger.info(response)
    
def lambda_handler(event, context):
    body = None
    
    # create dynamodb table
    create_db_table(table_name='Users')
    
    # list all s3 buckets
    body = list_all_buckets()
    
    # insert data into dynDB
    table_name = 'Users'
    batch_items = [
        {
            'id': 1,
            'name': 'xyz',
            'age': '20'
        },
        {
            'id': 2,
            'name': 'john',
            'age': '20'
        },

    ]
    insert_batch_data(table_name=table_name, data=batch_items)
    
    # get data from dynDB
    keys_list = [
        {
            'id':1,
        },
        {
            'id':2,
        },
    ]
    get_batch_items(table_name=table_name, keys_list=keys_list)
    
    # query data from dynDB
    query_table(table_name=table_name, id='id')
    
    # update data in dynDB
    key = {'id':1}
    change_param = 'age'
    val = 24
    update_item_in_table(table_name=table_name, key=key, change_param=change_param, val=val)
    
    # delete data in dynDB
    key = {'id':1}
    delete_item_in_table(table_name=table_name, key=key)
    
    # add object to bucket
    bucket_name = 'bucket'
    with open('aws.png', 'rb') as f:
        obj = f.read()
    key = 'aws.png'
    add_object_to_bucket(bucket_name=bucket_name, obj=obj, key=key)
    
    
    return {
        "statusCode": 200,
        "body": body
    }

def invoke_lambda(lambda_function_name:str='helloWorldLambda'):
    lambda_client = boto3.client('lambda')


    test_event = dict()

    response = lambda_client.invoke(
        FunctionName=lambda_function_name,
        Payload = json.dumps(test_event)
    )

    logger.info(response['Payload'])
    logger.info(response['Payload'].read().decode('utf-8'))

def describe_lambda_function(lambda_function_name:str='helloWorldLambda'):
    lambda_client = boto3.client('lambda')

    response = lambda_client.get_function(
        FunctionName=lambda_function_name
    )

    pprint(response)

def delete_lambda(lambda_function_name:str='helloWorldLambda'):
    lambda_client = boto3.client('lambda')
    response = lambda_client.delete_function(
        FunctionName=lambda_function_name
    )

    logger.info(response)