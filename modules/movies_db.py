from loguru import logger
from pprint import pprint
import json
from decimal import Decimal
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key


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

def update_movies_list(table_name:str, movies:dict) -> None:
    logger.debug('adding movies')
    db = boto3.resource('dynamodb')
    table = db.Table(table_name)
    
    for movie in movies:
        movie_title = movie['title']
        logger.debug(f'added movie {movie_title}')
        table.put_item(Item=movie)

def get_movie(table_name:str, title:str, year:int):
    db = boto3.resource('dynamodb')
    table = db.Table(table_name)
    try:
        response =  table.get_item(
            Key={
                'year':year,
                'title':title,
            }
        )
    except ClientError as e:
        logger.debug(e)
    
    else:
        return response['Item']

def update_movie(table_name:str, title:str, year:int, rating:str, plot:str):
    db = boto3.resource('dynamodb')
    table = db.Table(table_name)
    
    try:
        response = table.update_item(
            Key={
                'year':year,
                'title':title,
            },
            UpdateExpression='set info.rating=:r, info.plot=:p',
            ExpressionAttributeValues = {
                ':r':Decimal(rating),
                ':p':plot,
            },
            ReturnValues = 'UPDATED_NEW'
        )
    except ClientError as e:
        logger.debug(e)
    
    else:
        return response

def delete_movie(table_name:str, title:str, year:int):
    db = boto3.resource('dynamodb')
    table = db.Table(table_name)
    
    try:
        response = table.delete_item(
            Key={
                'year':year,
                'title':title,
            }
        )
    except ClientError as e:
        logger.debug(e)
    else:
        return response

def get_all_movies(table_name:str, year:int):
    db = boto3.resource('dynamodb')
    table = db.Table(table_name)
    
    try:
        response = table.query(
            KeyConditionExpression=Key('year').eq(year)
        )
    except ClientError as e:
        logger.debug(e)
    else:
        return response


# table = create_movies_table(table_name='movies')
# pprint(table.table_status)

# with open('resources/moviedata.json') as json_file:
#     movie_list = json.load(json_file, parse_float=Decimal)
    
# update_movies_list(table_name='movies', movies=movie_list)

# movie = get_movie(
#     table_name='movies',
#     title='Serenity',
#     year=2005
# )

# pprint(movie)

# update_movie(
#     table_name='movies',
#     title='Serenity',
#     year=2005,
#     rating='6.1',
#     plot="okay"
# )

# response = delete_movie(
#     table_name='movies',
#     title='Serenity',
#     year=2005
# )

# pprint(response)

response = get_all_movies(
    table_name='movies',
    year=2005
)

for movie in response['Items']:
    print(movie['title'])