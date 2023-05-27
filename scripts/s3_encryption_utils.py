import boto3
from loguru import logger
import json


def set_encryption(bucket_name:str):

    s3_client = boto3.client('s3')

    response = s3_client.put_bucket_encryption(
        Bucket=bucket_name,
        ServerSideEncryptionConfiguration={
            "Rules":[
                {"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm" : "AES256"}}
            ]
        }
    )

    logger.info(response)

def check_encryption(bucket_name:str):
    s3_client = boto3.client('s3')

    try:
        response = s3_client.get_bucket_encryption(Bucket=bucket_name)
        logger.info(response)

    except ClientError as e:
        logger.info("No encryption is available in this bucket")

def attach_default_encryption_policy(bucket_name:str):
    bucket_policy = {
        "Version": "2012-10-17",
        "Id": "Policy1670146696267",
        "Statement": [
            {
                "Sid": "Stmt1670146603372",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": f'arn:aws:s3:::{bucket_name}/*',
                "Condition": {
                    "StringNotEquals": {
                        "s3:x-amz-server-side-encryption": "AES256"
                    }
                }
            },
            {
                "Sid": "Stmt1670146692796",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": f'arn:aws:s3:::{bucket_name}/*',
                "Condition": {
                    "Null": {
                        "s3:x-amz-server-side-encryption": "true"
                    }
                }
            }
        ]
    }

    bucket_policy = json.dumps(bucket_policy)

    s3_cleint = boto3.client('s3')
    response = s3_cleint.put_bucket_policy(Bucket=bucket_name, Policy=bucket_policy)
    logger.info(response)

def get_bucket_policy(bucket_name:str):
    s3_client = boto3.client('s3')

    response = s3_client.get_bucket_policy(Bucket=bucket_name)
    logger.info(response['Policy'])

def delete_encryption(bucket_name:str):

    s3_client = boto3.client('s3')
    response = s3_client.delete_bucket_encryption(Bucket=bucket_name)
    logger.info(response)



delete_encryption(bucket_name='shrcbucketudemy')