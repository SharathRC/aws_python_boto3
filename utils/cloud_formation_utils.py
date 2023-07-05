import boto3
from loguru import logger
import pprint


def create_stack(stack_file_path:str, stack_name:str='MyStack'):
    cf_client = boto3.client('cloudformation')

    with open(stack_file_path, 'r') as f:
        template_str = f.read()
    
    
    params = [
        {
            'ParameterKey': 'HashKeyElementName',
            'ParameterValue':'EmployeeId'
        }
    ]

    response = cf_client.create_stack(
        StackName=stack_name,
        TemplateBody=template_str,
        Parameters=params

    )
    logger.info(response)

def describe_stack(stack_name:str):
    cf_client = boto3.client('cloudformation')

    response = cf_client.describe_stacks(
        StackName=stack_name
    )

    logger.info(response)

def get_template(stack_name:str):
    cf_client = boto3.client('cloudformation')

    response = cf_client.get_template(
        StackName=stack_name
    )

    pprint(response['TemplateBody'])

def delete_stack(stack_name:str):
    cf_client = boto3.client('cloudformation')

    response = cf_client.delete_stack(
        StackName=stack_name
    )

    logger.info(response)


# create_stack(stack_file_path='scripts/dynamodb.yml')
# get_template(stack_name='dynamostack')
# describe_stack(stack_name='createDyDBTable')
delete_stack(stack_name='createDyDBTable')