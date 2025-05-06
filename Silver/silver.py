from pyspark.sql import SparkSession
from pyspark.sql.functions import year, date_format, col
from pyspark.sql.types import StructType, StructField, StringType

# Step 1: Create Spark session with Hive support
spark = SparkSession.builder \
    .appName("SilverLayer") \
    .enableHiveSupport() \
    .getOrCreate()

WATCH_DIR = "../Bronze/data/stream"
STREAM_OUTPUT_DIR = "Silver/data/saved_tracks"

music_info_df = spark.read.parquet("Bronze/data/csv", header=True, inferSchema=True) #Load the csv file containing the data 

music_info_df.describe().show()



clean_df = music_info_df.withColumn("release_year", date_format("release_date", "yyyy"))\
    .drop("year")\
    .drop("release_date")\
    .drop("mode")\
    .drop("explicit")\
    .dropna()\
    .dropDuplicates()

df_1921 = clean_df.filter(col("year") == 1921)

clean_df.describe().show()

spark.sql("CREATE DATABASE IF NOT EXISTS spotifyInfo")

clean_df.write \
    .mode("overwrite") \
    .option("path", "data/musicInfo")\
    .format("parquet") \
    .saveAsTable("spotifyInfo.musicInfo")

spark.sql("SELECT * FROM spotifyInfo.musicInfo").describe().show()
spark.sql("SELECT * FROM spotifyInfo.musicInfo").show()

clean_df.write \
    .mode("overwrite")\
    .parquet("data/musicInfo")

import os


print("Bronze folder data : {}".format(os.listdir("Bronze/data/stream")))

schema = StructType([
    StructField("title", StringType(), True),
    StructField("artist", StringType(), True),
    StructField("album", StringType(), True),
    StructField("spotify_id", StringType(), True)
])

saved_tracks_df = spark.read.parquet("Bronze/data/stream")

cleaned_saved_tracks_df = saved_tracks_df.drop("artist")

cleaned_saved_tracks_df.write \
    .mode("overwrite")\
    .parquet("Silver/data/batch")



stream_saved_tracks_df = spark.readStream\
    .format("parquet") \
    .schema(saved_tracks_df.schema) \
    .load("Bronze/data/stream")

cleaned_stream_saved_tracks_df = stream_saved_tracks_df.drop("artist")

output = cleaned_stream_saved_tracks_df.writeStream\
    .format("parquet")\
    .option("path", STREAM_OUTPUT_DIR)\
    .option("checkpointLocation", "Silver/checkpoint") \
    .outputMode("append")\
    .start()

output.awaitTermination()
cleaned_saved_tracks_df.show()
