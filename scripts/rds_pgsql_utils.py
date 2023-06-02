import psycopg2
import boto3
from loguru import logger
import json
import mysql.connector as mc
from pprint import pprint


def create_db_instance(host:str, user:str, password:str, db_name:str, port:int=5432):
    try:
        conn = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port
        )

        conn.autocommit=True

        mycursor = conn.cursor()

        query = f"CREATE DATABASE {db_name}"

        mycursor.execute(query)
        logger.info("Database created")

    except Exception as e:
        logger.debug(e)
        logger.info("Failed to create database")

def check_connection(host:str, user:str, password:str, db_name:str, port:int=5432):
    try:
        conn = psycopg2.connect(
            database=db_name,
            user=user,
            password=password,
            host=host,
            port=port
        )

        logger.info("Database connected")

    except Exception as e:
        logger.debug(e)
        logger.info("Failed to connect the database")

def create_table(host:str, user:str, password:str, db_name:str, table:str, port:int=5432):
    try:
        conn = psycopg2.connect(
            database=db_name,
            user=user,
            password=password,
            host=host,
            port=port
        )

        cur = conn.cursor()
        query = f"CREATE TABLE {table}"
        cur.execute(query)
        conn.commit()
        logger.info("Table created")



    except Exception as e:
        logger.debug(e)
        logger.info("Can not create table")

def insert_data_in_table(host:str, user:str, password:str, db_name:str, query:str, port:int=5432):
    try:
        conn = psycopg2.connect(
            database=db_name,
            user=user,
            password=password,
            host=host,
            port=port
        )

        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        logger.info("Data has been added")

    except Exception as e:
        logger.debug(e)
        logger.info("Can not add teh data")

def select_data_in_table(host:str, user:str, password:str, db_name:str, query:str, port:int=5432):
    try:
        conn = psycopg2.connect(
            database=db_name,
            user=user,
            password=password,
            host=host,
            port=port
        )
        
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()

        for i, data in enumerate(rows):
            logger.info(f"entry: {i}")
            for val in data:
                logger.info(f"val1: {val}")

    except:
        print("Can not read the data")

def delete_data_in_table(host:str, user:str, password:str, db_name:str, query:str, port:int=5432):
    try:
        conn = psycopg2.connect(
            database=db_name,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cur = conn.cursor()

        cur.execute(query)
        conn.commit()
        logger.info("Data deleted")
        logger.info("Total number of row deleted " + str(cur.rowcount))

    except Exception as e:
        logger.debug(e)
        logger.info("Unable to delete the data")

def update_data_in_table(host:str, user:str, password:str, db_name:str, query:str, port:int=5432):
    try:
        conn = psycopg2.connect(
            database=db_name,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cur = conn.cursor()

        cur.execute(query)
        conn.commit()
        logger.info("Data updated")
        logger.info("Total Row Affected " + str(cur.rowcount))

    except Exception as e:
        logger.debug(e)
        logger.info("Unable to update the data")


table = 'Employee (ID INT PRIMARY KEY NOT NULL, NAME TEXT NOT NULL, EMAIL TEXT NOT NULL)'
query_insert = "INSERT INTO Employee (ID, NAME, EMAIL) VALUES (1, 'na1', 'n1@gmail.com')"
query_select = "SELECT * FROM Employee"
query_delete = "DELETE FROM Employee WHERE id=1"
query_update = "UPDATE Employee SET EMAIL = 'updated@gmail.com' WHERE id=1"
# create_db_instance(
#     host='pgsqludemyinstance.cja3bgbnq4oi.eu-central-1.rds.amazonaws.com',
#     user='postgres',
#     password='Keyboard!1234',
#     db_name='mydb',
# )

# check_connection(
#     host='pgsqludemyinstance.cja3bgbnq4oi.eu-central-1.rds.amazonaws.com',
#     user='postgres',
#     password='Keyboard!1234',
#     db_name='mydb',
# )

update_data_in_table(
    host='pgsqludemyinstance.cja3bgbnq4oi.eu-central-1.rds.amazonaws.com',
    user='postgres',
    password='Keyboard!1234',
    db_name='mydb',
    query=query_update
)