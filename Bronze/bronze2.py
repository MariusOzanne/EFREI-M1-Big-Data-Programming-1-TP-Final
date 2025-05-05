from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, year, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, TimestampType

# Créer la session Spark
spark = SparkSession.builder \
    .appName("BronzeLayer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .getOrCreate()

# Lecture du CSV
df = spark.read.csv("Source/data.csv", header=True, inferSchema=True)

# Conversion de la colonne timestamp
df = df.withColumn("timestamp", col("timestamp").cast(TimestampType()))
df = df.withColumn("annee", year(col("timestamp")))

# Ajout de la colonne période 30 ans
df = df.withColumn("periode_30_ans", when((col("annee") >= 1920) & (col("annee") < 1950), "1920-1950")
                                     .when((col("annee") >= 1950) & (col("annee") < 1980), "1950-1980")
                                     .when((col("annee") >= 1980) & (col("annee") < 2010), "1980-2010")
                                     .when((col("annee") >= 2010) & (col("annee") < 2040), "2010-2040")
                                     .otherwise("inconnue"))

# Écriture partitionnée du CSV
df.write.partitionBy("periode_30_ans").parquet("Bronze/data/csv_partitioned", mode="overwrite")

# Schéma JSON pour les messages Kafka
json_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("timestamp", StringType()),
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

    StructField("customer_rating", IntegerType()),
    StructField("discount_code", StringType()),
    StructField("tax_amount", DoubleType())
])

# Lecture du flux Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host.docker.internal:9092") \
    .option("subscribe", "transaction_log") \
    .option("startingOffsets", "earliest") \
    .load()

# Conversion en JSON puis parsing
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
parsed_df = json_df.select(from_json(col("json_str"), json_schema).alias("data")).select("data.*")

# Traitement de la colonne timestamp et ajout de la période
parsed_df = parsed_df.withColumn("timestamp", col("timestamp").cast(TimestampType()))
parsed_df = parsed_df.withColumn("annee", year(col("timestamp")))
parsed_df = parsed_df.withColumn("periode_30_ans", when((col("annee") >= 1920) & (col("annee") < 1950), "1920-1950")
                                               .when((col("annee") >= 1950) & (col("annee") < 1980), "1950-1980")
                                               .when((col("annee") >= 1980) & (col("annee") < 2010), "1980-2010")
                                               .when((col("annee") >= 2010) & (col("annee") < 2040), "2010-2040")
                                               .otherwise("inconnue"))

# Écriture du flux en Parquet partitionné
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "Bronze/data/stream_partitioned") \
    .option("checkpointLocation", "checkpoint") \
    .partitionBy("periode_30_ans") \
    .start()

query.awaitTermination()
