from pyspark.sql import SparkSession
from pyspark.sql.functions import col, least
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import udf
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import shutil
import time

# Path configuration
stream_features_dir = "Source\stream_features"
data_csv_path = "Source\data.csv"
output_folder = "ML\output"

# Main logic in a function so it can be triggered automatically
def run_recommendation():
    print("Running recommendation process...")

    # Initialize Spark
    spark = SparkSession.builder.appName("MusicRecommendation").getOrCreate()

    # Get latest saved_tracks_features file
    saved_tracks_files = [
        f for f in os.listdir(stream_features_dir)
        if f.startswith("saved_tracks_features_") and f.endswith(".csv")
    ]

    if not saved_tracks_files:
        print("No saved_tracks_features_*.csv found.")
        return

    latest_file = max(
        saved_tracks_files,
        key=lambda x: os.path.getmtime(os.path.join(stream_features_dir, x))
    )
    latest_file_path = os.path.join(stream_features_dir, latest_file)
    print(f"Using latest saved tracks file: {latest_file_path}")

    # Load CSVs
    saved_df = spark.read.option("header", True) \
                         .option("inferSchema", True) \
                         .option("multiLine", True) \
                         .option("quote", "\"") \
                         .option("escape", "\"") \
                         .csv(latest_file_path)

    full_df = spark.read.option("header", True) \
                        .option("inferSchema", True) \
                        .option("multiLine", True) \
                        .option("quote", "\"") \
                        .option("escape", "\"") \
                        .csv(data_csv_path)

    # Filter out already liked songs
    candidate_df = full_df.join(
        saved_df.select("id").withColumnRenamed("id", "saved_id"),
        full_df.id == col("saved_id"),
        "left_anti"
    )

    # Features
    features = ["valence", "acousticness", "danceability", "duration_ms", "energy",
                "instrumentalness", "liveness", "loudness", "speechiness", "tempo"]

    # Cast to Double
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

    # Clustering
    kmeans = KMeans(k=2, seed=42)
    model = kmeans.fit(saved_vector)
    centroids = model.clusterCenters()

    def squared_distance(vec, centroid):
        return float(vec.squared_distance(Vectors.dense(centroid)))

    dist0_udf = udf(lambda v: squared_distance(v, centroids[0]), DoubleType())
    dist1_udf = udf(lambda v: squared_distance(v, centroids[1]), DoubleType())

    scored = candidates_vector \
        .withColumn("dist0", dist0_udf(col("features"))) \
        .withColumn("dist1", dist1_udf(col("features"))) \
        .withColumn("min_dist", least(col("dist0"), col("dist1"))) \
        .orderBy("min_dist")

    # Save top recommendations
    recommendations = scored.select(
        "id", "name", "artists", "valence", "energy", "danceability", "tempo", "min_dist"
    )

    # Output file path
    today_str = datetime.today().strftime('%Y-%m-%d')
    os.makedirs(output_folder, exist_ok=True)
    final_csv_name = f"recommendations_{today_str}.csv"
    final_csv_path = os.path.join(output_folder, final_csv_name)
    temp_output_dir = os.path.join(output_folder, f"temp_output_{today_str}")

    recommendations.coalesce(1).write.option("header", True).mode("overwrite").csv(temp_output_dir)

    part_file = None
    for root, dirs, files in os.walk(temp_output_dir):
        for f in files:
            if f.startswith("part-") and f.endswith(".csv"):
                part_file = os.path.join(root, f)
                break

    if part_file:
        shutil.move(part_file, final_csv_path)
        shutil.rmtree(temp_output_dir)
        print(f"Final CSV saved as: {final_csv_path}")
    else:
        print("No part-*.csv file found in Spark output directory.")

    spark.stop()

# Watchdog file system event handler
class NewCSVHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.src_path.endswith(".csv") and "saved_tracks_features_" in event.src_path:
            print(f"\nNew CSV detected: {event.src_path}")
            run_recommendation()

# Main watcher logic
if __name__ == "__main__":
    print(f"Watching for new CSVs in: {stream_features_dir}")
    event_handler = NewCSVHandler()
    observer = Observer()
    observer.schedule(event_handler, path=stream_features_dir, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("Stopped watching.")

    observer.join()
