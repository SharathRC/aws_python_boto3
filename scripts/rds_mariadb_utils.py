import boto3
from loguru import logger
import json
import mariadb
from pprint import pprint


def check_connection(
    host:str,
    password:str,
    user:str='admin',
):
    try:
        db = mariadb.connect(
            host=host,
            user=user,
            password=password,
        )

        logger.info("There is a connection with the database")

    except mariadb.Error as e:
        logger.info("There is not any connection {} ".format(e))

def create_db(
    host:str,
    password:str,
    db_name:str,
    user:str='admin',
):
    try:
        db = mariadb.connect(
            host=host,
            user=user,
            password=password,
        )

        cur = db.cursor()
        cur.execute(f"CREATE DATABASE {db_name}")
        logger.info("DATABASE created ")


    except mariadb.Error as e:
        logger.info("Can not create database {} ".format(e))

def create_table(
    host:str,
    password:str,
    db_name:str,
    table:str,
    user:str='admin',
):
    try:
        db = mariadb.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
        )

        cur = db.cursor()
        cur.execute(f"CREATE TABLE {table}")
        logger.info("Table created ")


    except mariadb.Error as e:
        logger.info("Can not create table {} ".format(e))

def show_table(
    host:str,
    password:str,
    db_name:str,
    user:str='admin',
):
    try:
        db = mariadb.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
        )

        cur = db.cursor()
        cur.execute("SHOW TABLES")

        for data in cur:
            logger.info(data)


    except mariadb.Error as e:
        logger.info("Can not show the table {} ".format(e))

def insert_data_in_table(
    host:str,
    password:str,
    db_name:str,
    query:str,
    user:str='admin',
):
    try:
        db = mariadb.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
        )
        
        cur = db.cursor()
        cur.execute(query)
        db.commit()

        print("Data inserted")


    except mariadb.Error as e:
        logger.info("Unable to insert data {} ".format(e))

def get_data_from_table(
    host:str,
    password:str,
    db_name:str,
    table_name:str,
    user:str='admin',
):
    try:
        db = mariadb.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
        )
        
        cur = db.cursor()

        cur.execute("SELECT * FROM {} ".format(table_name))

        result = cur.fetchall()

        for data in result:
            print(data)


    except mariadb.Error as e:
        logger.info("Unable to get the data {} ".format(e))

def update_data_in_table(
    host:str,
    password:str,
    db_name:str,
    query:str,
    user:str='admin',
):
    try:
        db = mariadb.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
        )
        
        cur = db.cursor()
        cur.execute(query)

        db.commit()

        logger.info("record updated")


    except mariadb.Error as e:
        logger.info("Unable to update the data {} ".format(e))

def delete_data_in_table(
    host:str,
    password:str,
    db_name:str,
    query:str,
    user:str='admin',
):
    try:
        db = mariadb.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
        )
        
        cur = db.cursor()

        cur.execute(query)

        db.commit()
        logger.info('deleted entry')


    except mariadb.Error as e:
        logger.info("Unable to update the data {} ".format(e))



host = 'mariadbudemyinst.cja3bgbnq4oi.eu-central-1.rds.amazonaws.com'
password = 'password'
db_name = 'mydb'
user = 'admin'
table_name = 'Person'
table = f'{table_name} (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255),lastname VARCHAR(255) )'
query_insert = "INSERT INTO Person (name, lastname) VALUES ('nfgd', 'na2')"
query_update = "UPDATE Person SET name = 'newn' WHERE id=3"
query_delete = "DELETE FROM Person WHERE id = 3"

check_connection(host=host, password=password, user=user,)

# create_db(
#     host=host,
#     password=password,
#     db_name=db_name,
#     user=user,
# )

# create_table(
#     host=host,
#     password=password,
#     db_name=db_name,
#     user=user,
#     table=table,
# )

# show_table(host=host, password=password, db_name=db_name, user=user)
# insert_data_in_table(host=host, password=password, db_name=db_name, query=query_insert, user=user)
get_data_from_table(host=host, password=password, db_name=db_name, user=user, table_name=table_name)
# update_data_in_table(host=host, password=password, db_name=db_name, user=user, query=query_update)
delete_data_in_table(host=host, password=password, db_name=db_name, user=user, query=query_delete)
get_data_from_table(host=host, password=password, db_name=db_name, user=user, table_name=table_name)

