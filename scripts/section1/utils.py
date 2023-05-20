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
    
    for res in paginator.paginate(Scope=scope):
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

def detach_policy_with_arn(username:str, policy_arn:str):
    iam = boto3.client('iam')
    response = iam.detach_user_policy(
        UserName=username,
        PolicyArn=policy_arn,
    )
    logger.info(f"successfully detached policy {policy_arn}")
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

def detach_all_policies_from_user(username:str):
    logger.info(f"detaching all policies from user {username}")
    iam = boto3.client('iam')
    response = iam.list_attached_user_policies(UserName=username)
    logger.debug(response)
    
    attached_policies = response['AttachedPolicies']
    for policy in attached_policies:
        logger.debug(policy['PolicyName'])
        detach_policy_with_arn(
            username=username,
            policy_arn=policy['PolicyArn'],
        )
    
def create_group(group_name:str):
    try:
        iam = boto3.client('iam')
        iam.create_group(GroupName=group_name)
        logger.info(f'created group: {group_name}')
    except iam.exceptions.EntityAlreadyExistsException:
        logger.info('user with same name already exists')

def attach_policy_to_group_with_arn(group_name:str, policy_arn:str):
    iam = boto3.client('iam')
    response = iam.attach_group_policy(
        GroupName=group_name,
        PolicyArn=policy_arn,
    )
    logger.info('added policy to group')

def attach_policy_to_group(group_name:str, policy_name:str, scope:str='AWS'):
    policy_arn = get_policy_arn(policy_name=policy_name, scope=scope)
    
    if policy_arn:
        attach_policy_to_group_with_arn(group_name=group_name, policy_arn=policy_arn)
        logger.info(f"added policy: {policy_name} to group: {group_name}")
    else:
        logger.info("couldn't add policy to group")

def detach_policy_from_group_with_arn(group_name:str, policy_arn:str):
    iam = boto3.client('iam')
    response = iam.detach_group_policy(
        GroupName=group_name,
        PolicyArn=policy_arn,
    )
    logger.info('detached policy from group')
    logger.debug(response)

def detach_policy_from_group(group_name:str, policy_name:str,scope:str='AWS'):
    policy_arn = get_policy_arn(policy_name=policy_name, scope=scope)
    
    if policy_arn:
        detach_policy_from_group_with_arn(
            group_name=group_name,
            policy_arn=policy_arn,
        )
        logger.info(f"detached policy {policy_name} from group {group_name}")
    else:
        logger.info("failed to detach policy")
        

def add_user_to_group(usernames:list[str], group_name:str):
    iam = boto3.client('iam')
    for username in usernames:
        response = iam.add_user_to_group(
            UserName=username,
            GroupName=group_name,
        )
        logger.info(f"added user {username} to group {group_name}")

def create_access_key(username:str):
    iam = boto3.client('iam')
    response = iam.create_access_key(
        UserName=username
    )
    logger.debug(response)

def update_access_key(username:str, access_key:str, status:str='Inactive') -> None:
    iam = boto3.client('iam')
    response = iam.update_access_key(
        AccessKeyId=access_key,
        Status=status,
        UserName=username,
    )
    logger.debug(response)
    logger.info(f"updated access key status to {status}")

def delete_access_key(username:str, access_key:str):
    logger.info('deleting access key')
    iam = boto3.client('iam')
    
    try:
        response = iam.delete_access_key(
            UserName=username,
            AccessKeyId=access_key,
        )
        logger.debug(response)
    except iam.exceptions.NoSuchEntityException as e:
        logger.info('no access key found')

def create_login(username:str, password:str='password!1234'):
    iam = boto3.client('iam')
    
    login_profile = iam.create_login_profile(
        Password=password,
        PasswordResetRequired=False,
        UserName=username,
    )
    logger.debug(login_profile)
    logger.info(f'login profile created for user {username}')

def delete_login(username:str):
    logger.info('deleting login profile')
    iam = boto3.client('iam')
    try:
        iam.delete_login_profile(
            UserName=username,
        )
    except iam.exceptions.NoSuchEntityException as e:
        logger.info('no login profile found')

def remove_user_from_group(username:str, group_name:str):
    iam = boto3.resource('iam')
    group = iam.Group(group_name)
    
    response = group.remove_user(
        UserName=username,
    )
    
    logger.debug(response)
    logger.info(f'removed user {username} from group {group_name}')

def remove_user_from_all_groups(username:str):
    logger.info(f'removing user {username} from all groups')
    iam = boto3.client('iam')
    
    user_groups = iam.list_groups_for_user(UserName=username)
    logger.debug(f'belongs to groups: {user_groups}')
    
    for group_name in user_groups['Groups']:
        remove_user_from_group(username=username, group_name=group_name['GroupName'])

def delete_user(username:str, access_key:str=None):
    iam = boto3.client('iam')
    
    detach_all_policies_from_user(username=username)
    remove_user_from_all_groups(username=username)
    delete_login(username=username)
    
    if access_key:
        delete_access_key(username=username, access_key=access_key)
    
    response = iam.delete_user(
        UserName = username,
    )
    
    logger.debug(response)
    logger.info(f'deleted user: {username}')

# create_user(username="boto_gen_user2")
# list_users()
# update_user(old_username='boto_gen_user1', new_username='boto_gen_user_changed')
# create_full_access_policy()
# list_all_policies(scope='AWS')
# attach_policy_with_name(username="boto_gen_user2", policy_name="pyFullAccess", scope='Local')
# detach_policy_with_name(username="boto_gen_user2", policy_name="pyFullAccess", scope='Local')
# create_group(group_name="boto_gen_group1")
# attach_policy_to_group(
#     group_name='boto_gen_group1',
#     policy_name='AmazonS3FullAccess',
#     scope='AWS'
# )
# add_user_to_group(usernames=['boto_gen_user2'], group_name='boto_gen_group1')
# detach_policy_from_group(
#     group_name='boto_gen_group1',
#     policy_name='AmazonS3FullAccess',
#     scope='AWS'
# )
# create_access_key(username='boto_gen_user2')
# create_login(username='boto_gen_user2', password='password!1234')

# detach_all_policies_from_user(username='boto_gen_user2')
# remove_user_from_all_groups(username='boto_gen_user2')
# delete_login(username='boto_gen_user_changed')
# delete_user(username='boto_gen_user2', access_key='AKIASEIJOZWSKWXX77XR')