import boto3
from loguru import logger
from pprint import pprint
from boto3.dynamodb.conditions import Key


def create_db_table(table_name:str='Users') -> None:
    dynamodb = boto3.resource('dynamodb')

    # create dynamodb table
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'id',
                'KeyType': 'HASH'
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'id',
                'AttributeType': 'N'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1,
        }
    )

    logger.info("Table status:", table.table_status)
    
def insert_data(table_name:str, item:dict) -> None:
    try:
        db = boto3.resource('dynamodb')
        table = db.Table(table_name)
        
        table.put_item(
            Item=item
        )
        
        logger.info(f"added item to table {table_name}")
    
    except Exception as e:
        logger.info(e)

def insert_batch_data(table_name:str, batch_items:list[dict]) -> None:
    try:
        db = boto3.resource('dynamodb')
        table = db.Table(table_name)

        with table.batch_writer() as batch:
            for item in batch_items:
                batch.put_item(
                    Item=item
                )
        logger.info(f'added items to table {table_name}')
    except Exception as e:
        logger.info(e)

def describe_table(table_name:str) -> None:
    db = boto3.client('dynamodb')
    response = db.describe_table(
        TableName=table_name
    )

    pprint(response)

def update_table(table_name:str) -> None:
    try:
        db = boto3.client('dynamodb')
        response = db.update_table(
            TableName=table_name,
            BillingMode='PROVISIONED',
            
            ProvisionedThroughput={
                'ReadCapacityUnits':5,
                'WriteCapacityUnits':5,
            }
        )
    except Exception as e:
        logger.info(e)

def create_backup(table_name:str, backup_name:str) -> None:
    try:
        db = boto3.client('dynamodb')
        response = db.create_backup(
            TableName=table_name,
            BackupName=backup_name,
        )
        logger.info(f'created backup for table {table_name}')
    except Exception as e:
        logger.info(e)

def get_item(table_name:str, key:str, val:any) -> dict:
    try:
        db = boto3.resource('dynamodb')
        table = db.Table(table_name)
        response = table.get_item(
            Key = {
                key:val
            }
        )
        logger.debug(response)
        logger.info('item extracted')
        return response['Item']
    
    except Exception as e:
        logger.info(e)

def get_batch_items(table_name:str, keys_list:list[dict]) -> list[dict]:
    try:
        db = boto3.resource('dynamodb')
        response = db.batch_get_item(
            RequestItems = {
                table_name:{
                    'Keys':keys_list
                }
            }
        )

        return response['Responses']
    except Exception as e:
        logger.info(e)

def scan_table(table_name:str) -> None:
    try:
        db = boto3.resource('dynamodb')
        table = db.Table(table_name)
        response = table.scan()
        data = response['Items']
        pprint(data)
    except Exception as e:
        logger.info(e)

def query_table(table_name:str, id:str='id', id_val:any=1) -> None:
    dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table(table_name)

    resp = table.query(
        KeyConditionExpression=Key(id).eq(id_val)
    )

    if 'Items' in resp:
        logger.info(resp['Items'][0])

def update_item_in_table(table_name:str, key:dict, change_param:str='age', val:any=24) -> None:
    dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table(table_name)

    table.update_item(
        Key=key,
        UpdateExpression=f"set {change_param} = :g",
        ExpressionAttributeValues={
            ':g': val
        },
        ReturnValues="UPDATED_NEW"
    )

def delete_item_in_table(table_name:str, key:dict) -> None:
    dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table(table_name)

    response = table.delete_item(
        Key=key,
    )

    logger.info(response)
    


        


# insert_data(
#     table_name='employee',
#     item={
#         'emp_id':1425
#     }
# )

batch_items = [
    {
        'emp_id':157,
    },
    {
        'emp_id':789,
    },
    {
        'emp_id':452,
    },
    {
        'emp_id':246,
    },
    {
        'emp_id':74,
    },
    {
        'emp_id':865,
    },
]
# insert_batch_data(
#     table_name='employee',
#     batch_items=batch_items,
# )

# describe_table(
#     table_name='employee'
# )

# create_backup(
#     table_name='employee',
#     backup_name='employee_backup',
# )

# item = get_item(
#     table_name='employee',
#     key='emp_id',
#     val=452,
# )
# pprint(item)

# keys_list = [
#     {
#         'emp_id':157,
#     },
#     {
#         'emp_id':789,
#     },
#     {
#         'emp_id':74,
#     },
#     {
#         'emp_id':865,
#     },
# ]

# items = get_batch_items(
#     table_name='employee',
#     keys_list=keys_list,
# )
# pprint(items)

# scan_table(table_name='employee')

