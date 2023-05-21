import boto3
from loguru import logger
from pprint import pprint
import json
from decimal import Decimal


def create_movies_table(table_name:str) -> None:
    db = boto3.resource("dynamodb")
    table = db.create_table(
        TableName = table_name,
        KeySchema = [
            {
                'AttributeName': 'year',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'title',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
             {
                'AttributeName': 'year',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'title',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput = {
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    
    return table


# table = create_movies_table(table_name='movies')
# pprint(table.table_status)