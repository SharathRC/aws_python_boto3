import boto3
from loguru import logger
import json
import mysql.connector as mc



def create_mysql_instance(rds_name:str):
    rds_client = boto3.client('rds')


    response = rds_client.create_db_instance(
        DBName=rds_name,
        DBInstanceIdentifier=rds_name,
        AllocatedStorage=20,
        DBInstanceClass='db.t3.micro',
        Engine='MySQL',
        MasterUsername=rds_name,
        MasterUserPassword='Keyboard!1234',
        Port=3306,
        EngineVersion='8.0.32',
        PubliclyAccessible=True,
        StorageType='gp2'
    )

    logger.info(response)

def create_db(host:str, user:str, password:str, db_name:str):
    try:
        mydb = mc.connect(
            host=host,
            user=user,
            password=password,
        )

        cursor = mydb.cursor()

        cursor.execute("CREATE DATABASE {} ".format(db_name))
        logger.info("Database created ")


    except mc.Error as e:
        print("Failed to create database {} ".format(e))

def check_connection(host:str, user:str, password:str, db_name:str):
    try:
        mydb = mc.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
        )

        print("Connection created")



    except mc.Error as e:
        print("There is no connection {} ".format(e))

# create_mysql_instance(rds_name='rdstuts')
check_connection(
    host='rdstuts.cja3bgbnq4oi.eu-central-1.rds.amazonaws.com',
    user='rdstuts',
    password='Keyboard!1234',
    db_name='dbtuts'
)