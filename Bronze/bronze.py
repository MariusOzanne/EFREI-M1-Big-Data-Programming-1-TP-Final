from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, TimestampType, MapType, BooleanType, LongType

spark = SparkSession.builder \
    .appName("BronzeLayer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .getOrCreate()

df = spark.read.csv("Source\data.csv", header=True, inferSchema=True)

df.write.parquet("Bronze/data/csv", mode="overwrite")

json_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("timestamp", StringType()),  # Or TimestampType() if parsed with `to_timestamp`
    StructField("user_id", StringType()),
    StructField("user_name", StringType()),
    StructField("product_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("transaction_type", StringType()),
    StructField("status", StringType()),
    
    StructField("location", StructType([
        StructField("city", StringType()),
        StructField("country", StringType())
    ])),

    StructField("payment_method", StringType()),
    StructField("product_category", StringType()),
    StructField("quantity", IntegerType()),

    StructField("shipping_address", StructType([
        StructField("street", StringType()),
        StructField("zip", StringType()),
        StructField("city", StringType()),
        StructField("country", StringType())
    ])),

    StructField("device_info", StructType([
        StructField("os", StringType()),
        StructField("browser", StringType()),
        StructField("ip_address", StringType())
    ])),

    StructField("customer_rating", IntegerType()),  # Can be null
    StructField("discount_code", StringType()),     # Can be null
    StructField("tax_amount", DoubleType())
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host.docker.internal:9092") \
    .option("subscribe", "transaction_log") \
    .option("startingOffsets", "earliest") \
    .load()

# Extract JSON string from Kafka 'value' column
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

# Parse JSON string into columns
parsed_df = json_df.select(from_json(col("json_str"), json_schema).alias("data")).select("data.*")

query = parsed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "Bronze/data/stream") \
    .option("checkpointLocation", "checkpoint") \
    .start()

query.awaitTermination()