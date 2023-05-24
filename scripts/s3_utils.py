import boto3
import json
from loguru import logger

def create_bucket(bucket_name:str, acl:str='private') -> None:
    bucket = boto3.resource('s3')
    
    response = bucket.create_bucket(
        Bucket = bucket_name,
        ACL = acl, # public-read public-read-write
        CreateBucketConfiguration = {
            'LocationConstraint':'eu-central-1'
        }
    )
    
    logger.debug(response)

def add_object_to_bucket(bucket_name:str, obj:any, key:str, acl:str='private') -> None:
    client = boto3.client('s3')

    response = client.put_object(
        ACL = acl,
        Bucket = bucket_name,
        Body = obj,
        Key = key,
    )
    
    logger.debug(response)

def list_all_buckets():
    resource = boto3.resource('s3')
    buckets = resource.buckets.all()
    
    logger.info('listing all buckets')
    for bucket in buckets:
        logger.info(bucket.name)

def clean_up(bucket):
    for object in bucket.objects.all():
        object.delete()
    
    for object_ver in bucket.object_versions.all():
        object_ver.delete()
    
    logger.info(f"cleaned up bucker {bucket.name}")

def delete_bucket(bucket_name:str):
    resource = boto3.resource('s3')
    bucket = resource.Bucket(bucket_name)
    
    clean_up(bucket=bucket)
    
    bucket.delete()
    
    logger.info(f'deleted bucket {bucket_name}')


# create_bucket(bucket_name='awsbotolessonbucket')
# img_file = open('resources/img.png', 'rb')
# add_object_to_bucket(
#     bucket_name='awsbotolessonbucket',
#     obj=img_file.read(),
#     key='img',
# )
# list_all_buckets()

delete_bucket(bucket_name='awsbotolessonbucket')