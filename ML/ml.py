from pyspark.sql import SparkSession
from pyspark.sql.functions import col, least
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import udf
from datetime import datetime
import os
import shutil

# Initialize Spark session
spark = SparkSession.builder\
    .appName("MusicRecommendation")\
    .getOrCreate()

# Correct absolute file paths (relative to current working directory)
saved_tracks_features = "Source\stream_features\saved_tracks_features_2025-05-04.csv"
data_csv_file = "Silver/data/music_info"

# Load CSVs
saved_df = spark.read.option("header", True) \
                     .option("inferSchema", True) \
                     .option("multiLine", True) \
                     .option("quote", "\"") \
                     .option("escape", "\"") \
                     .csv(saved_tracks_features)

full_df = spark.read.option("header", True) \
                    .option("inferSchema", True) \
                    .option("multiLine", True) \
                    .option("quote", "\"") \
                    .option("escape", "\"") \
                    .parquet(data_csv_file)

# Filter out already liked songs
candidate_df = full_df.join(saved_df.select("id").withColumnRenamed("id", "saved_id"),
                            full_df.id == col("saved_id"), "left_anti")

# Features to use
features = ["valence", "acousticness", "danceability", "duration_ms", "energy",
            "instrumentalness", "liveness", "loudness", "speechiness", "tempo"]

# Cast features to DoubleType
for f in features:
    saved_df = saved_df.withColumn(f, col(f).cast(DoubleType()))
    candidate_df = candidate_df.withColumn(f, col(f).cast(DoubleType()))

# Drop nulls
saved_df_clean = saved_df.dropna(subset=features)
candidate_df_clean = candidate_df.dropna(subset=features)

# Assemble vectors
vector_assembler = VectorAssembler(inputCols=features, outputCol="features")
saved_vector = vector_assembler.transform(saved_df_clean).select("id", "features")
candidates_vector = vector_assembler.transform(candidate_df_clean)

# KMeans clustering
kmeans = KMeans(k=2, seed=42)
model = kmeans.fit(saved_vector)
centroids = model.clusterCenters()

# Distance UDFs
def squared_distance(vec, centroid):
    return float(vec.squared_distance(Vectors.dense(centroid)))

dist0_udf = udf(lambda v: squared_distance(v, centroids[0]), DoubleType())
dist1_udf = udf(lambda v: squared_distance(v, centroids[1]), DoubleType())

# Score candidates
scored = candidates_vector \
    .withColumn("dist0", dist0_udf(col("features"))) \
    .withColumn("dist1", dist1_udf(col("features"))) \
    .withColumn("min_dist", least(col("dist0"), col("dist1"))) \
    .orderBy("min_dist")

# Show top 10 recommended songs
scored.select("name", "artists", "valence", "energy", "danceability", "tempo", "min_dist") \
      .show(10, truncate=False)

# Select only the top 10 for export
recommendations = scored.select(
    "id", "name", "artists", "valence", "energy", "danceability", "tempo", "min_dist"
).limit(10)

# Output file
today_str = datetime.today().strftime('%Y-%m-%d')
final_csv_name = f"recommendations_{today_str}.csv"

# Temporary folder for Spark output
temp_output_dir = "temp_output_csv"
recommendations.coalesce(1).write.option("header", True).mode("overwrite").csv(temp_output_dir)

# Move part file to flat CSV
for f in os.listdir(temp_output_dir):
    if f.startswith("part-") and f.endswith(".csv"):
        shutil.move(os.path.join(temp_output_dir, f), final_csv_name)
        break

# Clean up
shutil.rmtree(temp_output_dir)

print(f"Saved top 10 recommendations to: {final_csv_name}")