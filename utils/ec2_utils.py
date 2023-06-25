import boto3
from pprint import pprint
from loguru import logger


def describe_ec2(region_name:str='eu-central-1'):
    ec2_client = boto3.client('ec2',region_name=region_name)
    response = ec2_client.describe_instances()
    pprint(response)

    response = ec2_client.describe_security_groups()
    pprint(response)

def create_key_pair(key_name:str, key_type:str='rsa', region_name:str='euro-central-1'):
    ec2_client = boto3.client('ec2', region_name=region_name)

    resp = ec2_client.create_key_pair(
        KeyName = key_name,
        KeyType=key_type,
    )

    logger.info(resp['KeyMaterial'])

    #store the pem file

    file = open(f'{key_name}_{region_name}.pem', 'w')
    file.write(resp['KeyMaterial'])
    file.close()

def create_security(group_name:str, vpc_id:str, desc:str='sample desc', region_name:str='euro-central-1'):
    ec2_client = boto3.client('ec2', region_name=region_name)

    response = ec2_client.create_security_group(
        Description=desc,
        GroupName=group_name,
        VpcId=vpc_id,
    )

    logger.info(response)
    
def set_inbound_rules(
    group_id:str, 
    from_ports:list[int], 
    to_port1s:list[int],
    protocol:str='tcp',
    region_name:str='euro-central-1',
):
    ec2_client = boto3.client('ec2', region_name=region_name)
    
    ip_permissions = []
    for from_port, to_port in zip(from_ports, to_port1s):
        ip_permissions.append(
            {
                'FromPort': from_port,
                'ToPort': to_port,
                'IpProtocol': protocol,
                'IpRanges': [{'CidrIp':'0.0.0.0/0', 'Description':'desc'}]
            }
        )

    response = ec2_client.authorize_security_group_ingress(
        GroupId=group_id,
        IpPermissions=ip_permissions,
    )

    logger.info(response)

def create_ec2_instance(
    image_id:str, 
    key_name:str, 
    security_groups:list[str], 
    instance_type:str='t3.micro', 
    region_name:str='euro-central-1',
):
    
    ec2_resource = boto3.resource('ec2', region_name=region_name)

    response = ec2_resource.create_instances(
        ImageId=image_id,
        MinCount=1,
        MaxCount=1,
        InstanceType=instance_type,
        KeyName=key_name,
        SecurityGroups=security_groups
    )

    logger.info(response)

def get_instance_ip(
    instance_id:str, 
    region_name:str='euro-central-1',
):
    ec2_client = boto3.client('ec2', region_name=region_name)

    reservations = ec2_client.describe_instances(InstanceIds=[instance_id]).get('Reservations')

    for reservation in reservations:
        for instance in reservation['Instances']:
            logger.info(f"ip: {instance.get('PublicIpAddress')}")


def list_instances(
    region_name:str='euro-central-1',
):
    ec2_client = boto3.client('ec2', region_name=region_name)

    reservations = ec2_client.describe_instances().get('Reservations')

    for reservation in reservations:
        for instance in reservation['Instances']:
            instance_id = instance['InstanceId']
            instance_type = instance['InstanceType']
            public_ip = instance['PublicIpAddress']
            private_ip = instance['PrivateIpAddress']

            logger.info(f"{instance_id}, {instance_type}, {public_ip}, {private_ip}")

def stope_instance(
    instance_id:str,
    region_name:str='euro-central-1',
):
    ec2_client = boto3.client('ec2', region_name=region_name)
    response = ec2_client.stop_instances(InstanceIds=[instance_id])

    logger.info(response)

def terminate_instance(
    instance_id:str,
    region_name:str='euro-central-1',
):
    ec2_client = boto3.client('ec2', region_name=region_name)
    response = ec2_client.terminate_instances(InstanceIds=[instance_id])
    logger.info(response)

def delete_security_group(
    group_id:str,
    region_name:str='euro-central-1',
):
    ec2_client = boto3.client('ec2', region_name=region_name)
    response = ec2_client.delete_security_group(GroupId=group_id)
    logger.info(response)

def delete_key_pair(
    key_name:str,
    region_name:str='euro-central-1',
):
    ec2_client = boto3.client('ec2', region_name=region_name)
    response = ec2_client.delete_key_pair(KeyName=key_name)
    logger.info(response)





region_name='eu-north-1'
security_group_id = 'sg-0fb2790a0df054c01'
instance_id = 'i-01751f71eb80c0ae5'

# create_key_pair(key_name='udemy_aws_key', region_name=region_name)
# create_security(
#     desc='security desc', 
#     group_name='pygroup', 
#     vpc_id='vpc-0b2118a0fc3539330', 
#     region_name=region_name,
# )

# set_inbound_rules(
#     region_name=region_name,
#     group_id=security_group_id, 
#     from_ports=[80,22], 
#     to_port1s=[80,22], 
#     protocol='tcp',
# )

# create_ec2_instance(
#     region_name=region_name, 
#     image_id='ami-0989fb15ce71ba39e', #linux ubuntu 22.04 
#     key_name='udemy_aws_key', 
#     security_groups=['pygroup'], 
#     instance_type='t3.micro',
# )

# get_instance_ip(instance_id=instance_id, region_name=region_name)
# list_instances(region_name=region_name)
# stope_instance(instance_id=instance_id, region_name=region_name)
# terminate_instance(instance_id=instance_id, region_name=region_name)
describe_ec2(region_name=region_name)
