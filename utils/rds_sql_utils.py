import boto3
from loguru import logger
import json
import mysql.connector as mc
from pprint import pprint


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
        logger.info("Failed to create database {} ".format(e))

def check_connection(host:str, user:str, password:str, db_name:str):
    try:
        mydb = mc.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
        )

        logger.info("Connection created")



    except mc.Error as e:
        logger.info("There is no connection {} ".format(e))

def create_table(host:str, user:str, password:str, db_name:str, table:str):
    try:
        mydb = mc.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
        )


        mycursor = mydb.cursor()

        mycursor.execute(f"CREATE TABLE {table}")
        logger.info("Table is created")



    except mc.Error as e:
        logger.info("Failed to create table {} ".format(e))

def show_table(host:str, user:str, password:str, db_name:str):
    try:
        mydb = mc.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
        )


        mycursor = mydb.cursor()

        mycursor.execute("SHOW TABLES")

        for table in mycursor:
            logger.info(table)


    except mc.Error as e:
        logger.info("Can not show the tables {} ".format(e))

def insert_data_to_table(host:str, user:str, password:str, db_name:str, query:str, value:str):
    try:
        mydb = mc.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
        )

        mycursor = mydb.cursor()

        mycursor.execute(query, value)

        mydb.commit()
        logger.info("Data Inserted")


    except mc.Error as e:
        logger.info("Failed to add data {} ".format(e))

def show_data_in_table(host:str, user:str, password:str, db_name:str, tablename:str):
    try:
        mydb = mc.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
        )

        mycursor = mydb.cursor()

        mycursor.execute("SELECT * FROM {} ".format(tablename))

        result = mycursor.fetchall()

        for data in result:
            logger.info(data)


    except mc.Error as e:
        logger.info("Can not show the data ".format(e))

def update_data_in_table(host:str, user:str, password:str, db_name:str, query:str):
    try:
        mydb = mc.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
        )

        mycursor = mydb.cursor()

        mycursor.execute(query)
        mydb.commit()
        logger.info(mycursor.rowcount, "record affected")

    except mc.Error as e:
        logger.info("Can not update data {} ".format(e))

def delete_data_in_table(host:str, user:str, password:str, db_name:str, query:str):
    try:
        mydb = mc.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
        )
        
        mycursor = mydb.cursor()

        mycursor.execute(query)

        mydb.commit()

        logger.info(f'{mycursor.rowcount} record affected')


    except mc.Error as e:
        logger.info("Can not delete the item {} ".format(e))

def describe_rds_instance(db_identifier:str):
    rds_client = boto3.client('rds')

    response = rds_client.describe_db_instances(
        DBInstanceIdentifier = db_identifier
    )

    pprint(response)

def delete_rds_instance(db_identifier:str):
    rds_client = boto3.client('rds')

    response = rds_client.delete_db_instance(
        DBInstanceIdentifier=db_identifier,
        SkipFinalSnapshot=False,
        FinalDBSnapshotIdentifier=f"{db_identifier}-final-snapshot",
        DeleteAutomatedBackups=True

    )

    logger.info(response)

# create_mysql_instance(rds_name='rdstuts')
# table = 'Person (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), lastname VARCHAR(255))'

# query = "INSERT INTO Person (name, lastname) VALUES (%s, %s)"
# value = ('name1', 'lastname1')

# query = "UPDATE Person SET name='updated' WHERE id='1'"
# query = "DELETE FROM Person WHERE id='1'"
# delete_data_in_table(
#     host='rdstuts.cja3bgbnq4oi.eu-central-1.rds.amazonaws.com',
#     user='rdstuts',
#     password='Keyboard!1234',
#     db_name='dbtuts',
#     query=query,
# )

# delete_rds_instance(db_identifier='rdstuts')

# show_data_in_table(
#     host='rdstuts.cja3bgbnq4oi.eu-central-1.rds.amazonaws.com',
#     user='rdstuts',
#     password='Keyboard!1234',
#     db_name='dbtuts',
#     tablename='Person',
# )