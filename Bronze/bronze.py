from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

spark = SparkSession.builder \
    .appName("BronzeLayer") \
    .getOrCreate()

WATCH_DIR = "Source/stream"
STREAM_OUTPUT_DIR = "Bronze/data/stream"

print(r"BEGGINING EXTRACTION OF THE MUSIC FEATURES")
features_df = spark.read.csv("Source/data.csv", header=True, inferSchema=True)
features_df.write.parquet("Bronze/data/csv", mode="overwrite")
print(r"MUSIC FEATURES SUCCESSFULLY EXTRACTED!")

print(r"ATTEMPTING TO READ THE SAVED TRACKS STREAM")
df = spark.readStream \
    .option("header", "true") \
    .format("csv") \
    .schema("title STRING, artist STRING, album STRING, spotify_id STRING") \
    .load(WATCH_DIR)

print(r"SAVED TRACKS STREAM READ SUCCESSFULLY!")


print(r"ATTEMPTING TO WRITE THE STREAMED DATA")
output = df.writeStream\
    .format("parquet")\
    .option("path", STREAM_OUTPUT_DIR)\
    .option("checkpointLocation", "Bronze/checkpoint") \
    .outputMode("append") \
    .start()
print(r"STREAMED DATA WRITTEN SUCCESSFULLY")
output.awaitTermination()

