import boto3
import json
from loguru import logger

def create_bucket(bucket_name:str, acl:str='private', loc:str='eu-central-1') -> None:
    bucket = boto3.resource('s3')
    
    response = bucket.create_bucket(
        Bucket = bucket_name,
        ACL = acl, # public-read public-read-write
        CreateBucketConfiguration = {
            'LocationConstraint':loc
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

def upload_files(file_name, bucket_name, object_name=None, args=None):
    resource = boto3.resource('s3')
    if object_name is None:
        object_name = file_name

    resource.meta.client.upload_file(file_name, bucket_name, object_name, ExtraArgs=args)
    logger.info(f"{file_name} has been uploaded to {bucket_name} bucket")

def download_file(bucket_name:str, file_name:str):
    s3_resource = boto3.resource('s3')

    s3_object = s3_resource.Object(bucket_name, file_name)

    s3_object.download_file(file_name)

    print("File has been downloaded")

def list_objects_in_bucket(bucket_name:str):
    s3_resource = boto3.resource('s3')
    s3_bucket = s3_resource.Bucket(bucket_name)

    print("Listing Bucket Files or Objects")

    for obj in s3_bucket.objects.all():
        logger.info(obj.key)

def find_objects(bucket_name:str, prefix:str):
    s3_resource = boto3.resource('s3')
    s3_bucket = s3_resource.Bucket(bucket_name)

    logger.info("Listing Filtered File")

    for obj in s3_bucket.objects.filter(Prefix=prefix):
        logger.info(obj.key)

def get_object_summary(bucket_name:str, obj:str):
    s3 = boto3.resource('s3')
    object_summary = s3.ObjectSummary(bucket_name, obj)
    
    logger.info("Object summary")
    logger.info(object_summary.bucket_name)
    logger.info(object_summary.key)

def copy_obj(bucket_name:str, key:str, new_bucket:str, new_key:str):
    s3 = boto3.resource('s3')
    copy_source = {
        'Bucket':bucket_name,
        'Key':key
    }

    s3.meta.client.copy(copy_source, new_bucket, new_key)
    
    logger.info('copied object to new bucket')

def delete_objects(bucket_name:str, keys:list[str]):
    s3_resource = boto3.resource('s3')
    #delete multiple files or objects
    
    for key in keys:
        s3_resource.Object(bucket_name, key).delete()

    logger.info('deleted object/s')


# create_bucket(bucket_name='shrcbucketudemy2')
# img_file = open('resources/img.png', 'rb')
# add_object_to_bucket(
#     bucket_name='awsbotolessonbucket',
#     obj=img_file.read(),
#     key='img',
# )
# list_all_buckets()

# delete_bucket(bucket_name='awsbotolessonbucket')
# upload_files(file_name='requirements.txt', bucket_name='shrcbucketudemy')
# download_file(bucket_name='shrcbucketudemy', file_name='requirements.txt')
# list_objects_in_bucket(bucket_name='shrcbucketudemy')
# find_objects(bucket_name='shrcbucketudemy', prefix='req')
# get_object_summary(bucket_name='shrcbucketudemy', obj='requirements.txt')

# copy_obj(
#     bucket_name='shrcbucketudemy',
#     key='requirements.txt',
#     new_bucket='shrcbucketudemy2',
#     new_key='requirements.txt',
# )

# delete_objects(
#     bucket_name='shrcbucketudemy2',
#     keys=[
#         'requirements.txt'
#     ]
# )
