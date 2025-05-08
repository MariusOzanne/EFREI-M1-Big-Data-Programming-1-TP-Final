import os
from dotenv import load_dotenv
from pyspark.sql import DataFrame

load_dotenv()

def write_to_parquet(df: DataFrame, path: str, write_mode = "overwrite"):
  df.write\
    .parquet(path, mode= write_mode)\
      

def write_to_mysql(df, table_name, db_name="music_marts", mode="overwrite"):

  user = os.environ.get("MYSQL_USER")
  password = os.environ.get("MYSQL_PASSWORD")

  if not user:
    raise ValueError("MYSQL_USER and/or MYSQL_PASSWORD environment variables are not set.")
  
  df.write \
    .format("jdbc") \
    .option("url", f"jdbc:mysql://localhost:3306/{db_name}") \
    .option("dbtable", table_name) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .mode(mode) \
    .save()