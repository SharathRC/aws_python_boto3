import boto3
from loguru import logger
from pprint import pprint


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

