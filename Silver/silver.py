from pyspark.sql import SparkSession
from pyspark.sql.functions import year, date_format, col
from pyspark.sql.types import StructType, StructField, StringType
import os

# Create Spark session with Hive support 
spark = SparkSession.builder \
    .appName("SilverLayer") \
    .enableHiveSupport() \
    .getOrCreate()

# Have all the directories accessible and easily modifyable 
WATCH_DIR = "../Bronze/data/stream"
STREAM_OUTPUT_DIR = "Silver/data/saved_tracks"

#  Preparing the data to modify
music_info_df = spark.read.parquet("Bronze/data/csv", header=True, inferSchema=True)

#music_info_df.describe().show()


# Droping the unnecessary columns, duplicates as well as comumns containing null values to make all the data uniform 
clean_df = music_info_df.withColumn("release_year", date_format("release_date", "yyyy"))\
    .drop("year")\
    .drop("release_date")\
    .drop("mode")\
    .drop("explicit")\
    .dropna()\
    .dropDuplicates()

#clean_df.describe().show()

#spark.sql("CREATE DATABASE IF NOT EXISTS spotifyInfo")

#clean_df.write \
#    .mode("overwrite") \
#    .option("path", "data/music_info")\
#    .format("parquet") \
#    .saveAsTable("spotifyInfo.musicInfo")
#
#spark.sql("SELECT * FROM spotifyInfo.musicInfo").describe().show()
#spark.sql("SELECT * FROM spotifyInfo.musicInfo").show()

# Data cleaning done
clean_df.write \
    .mode("overwrite")\
    .parquet("Silver/data/music_info")


print("Bronze folder data : {}".format(os.listdir("Bronze/data/stream")))

# The SAVED_TRACK_STREAM directory is not readable with infer schema. One has to be made by hand.
# ---->Change this if the structure of the data in the SAVED_TRACK_STREAM directory changes<----
schema = StructType([
    StructField("title", StringType(), True),
    StructField("artist", StringType(), True),
    StructField("album", StringType(), True),
    StructField("spotify_id", StringType(), True)
])

# Data from a streamed directory has to be retrieved both as a batch and as a stream to get the new data streamed.
# Just reading the stream won't allow you to have access to the previously written files in the directory

saved_tracks_df = spark.read.parquet("Bronze/data/stream")

cleaned_saved_tracks_df = saved_tracks_df.drop("album")\
    .dropna()\
    .dropDuplicates()

cleaned_saved_tracks_df.write \
    .mode("overwrite")\
    .parquet(STREAM_OUTPUT_DIR)



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

cleaned_saved_tracks_df.show()
output.awaitTermination()
