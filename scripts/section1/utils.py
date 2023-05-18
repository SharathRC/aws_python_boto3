import boto3
import json
from loguru import logger

def create_user(username:str):
    iam = boto3.client('iam')
    response = iam.create_user(UserName=username)
    logger.debug(response)

def list_users():
    iam = boto3.client('iam')
    paginator = iam.get_paginator('list_users')
    
    for res in paginator.paginate():
        for user in res['Users']:
            username = user['UserName']
            arn = user['Arn']
            logger.debug(f"username: {username}, arn: {arn}")
    
def update_user(old_username:str, new_username:str):
    iam = boto3.client('iam')
    response = iam.update_user(
        UserName=old_username,
        NewUserName=new_username,
    )
    logger.debug(response)

def create_full_access_policy():
    iam = boto3.client('iam')
    user_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "*",
                "Resource": "*"
            }
        ]
    }
    
    response = iam.create_policy(
        PolicyName='pyFullAccess',
        PolicyDocument=json.dumps(user_policy),
    )
    
    logger.info("successfully created policy")
    logger.debug(response)

def list_all_policies(scope:str='AWS'):
    iam = boto3.client('iam')
    paginator = iam.get_paginator('list_policies')
    
    for res in paginator.paginate(Scope=scope): # AWS, Local 
        for policy in res['Policies']:
            policy_name = policy['PolicyName']
            arn = policy['Arn']
            logger.debug(f'policy name: {policy_name}, arn: {arn}')

def get_policy_arn(policy_name:str, scope:str='AWS') -> str:
    iam = boto3.client('iam')
    paginator = iam.get_paginator('list_policies')
    
    for res in paginator.paginate(Scope='Local'):
        for policy in res['Policies']:
            if policy['PolicyName'] == policy_name:
                policy_arn = policy['Arn']
                logger.debug(f'found policy {policy_name}, arn: {policy_arn}')
                return policy_arn
    
    logger.debug("policy not found")
    return None

def attach_policy_with_arn(username:str, policy_arn:str):
    iam = boto3.client('iam')
        
    response = iam.attach_user_policy(
        UserName=username,
        PolicyArn=policy_arn,
    )
    
    logger.info("successfully added policy to user")
    logger.debug(response)

def attach_policy_with_name(username:str, policy_name:str, scope:str='AWS'):
    iam = boto3.client('iam')
    
    policy_arn = get_policy_arn(policy_name=policy_name, scope=scope)
    
    if policy_arn:
        attach_policy_with_arn(username=username, policy_arn=policy_arn)
    else:
        logger.info("Couldn't add policy to user")

def detach_policy_with_arn(username:str, policy_arn:str, scope:str='AWS'):
    iam = boto3.client('iam')
    response = iam.detach_user_policy(
        UserName=username,
        PolicyArn=policy_arn,
    )
    logger.info("successfully dettached policy")
    logger.debug(response)

def detach_policy_with_name(username:str, policy_name:str, scope:str='AWS'):    
    policy_arn = get_policy_arn(
        policy_name=policy_name,
        scope=scope,
    )
    
    if policy_arn:
        detach_policy_with_arn(
            username=username,
            policy_arn=policy_arn,
            scope=scope,
        )
    else:
        logger.info("couldn't dettach policy")
    
    
    

# create_user(username="boto_gen_user2")
# list_users()
# update_user(old_username='boto_gen_user1', new_username='boto_gen_user_changed')
# create_full_access_policy()
# list_all_policies(scope:str='AWS')
# attach_policy_with_name(username="boto_gen_user2", policy_name="pyFullAccess", scope='Local')
detach_policy_with_name(username="boto_gen_user2", policy_name="pyFullAccess", scope='Local')

