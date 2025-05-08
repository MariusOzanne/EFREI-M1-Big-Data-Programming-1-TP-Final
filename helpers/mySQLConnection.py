import mysql.connector
from mysql.connector.connection import MySQLCursor, MySQLConnection

def connect(host_str: str, username_str: str, password_str: str):
    mydb = mysql.connector.connect(
    host = host_str,
    user = username_str,
    password = password_str
    )
    return mydb

def getCursor(connection: MySQLConnection):
    return connection.cursor()

def create_database(name: str, cursor: MySQLCursor):
    cursor.execute("CREATE DATABASE {}".format(name))