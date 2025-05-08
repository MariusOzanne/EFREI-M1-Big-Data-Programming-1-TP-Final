import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col, avg
from pyspark.sql.types import StructType, StructField, StringType
from helpers import sparkHelpers as Helpers
import ast

spark = SparkSession.builder \
    .appName("GoldLayer") \
    .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
    .getOrCreate()

MUSIC_INFO_PATH = "Silver/data/music_info"
SAVED_TRACKS_PATH = "Silver/data/saved_tracks"
OUTPUT_FILE = "Gold/gold_data.csv"

saved_tracks_schema = StructType([
    StructField("title", StringType(), True),
    StructField("artist", StringType(), True),
    StructField("spotify_id", StringType(), True)
])

features_df = spark.read.parquet(MUSIC_INFO_PATH)

print("FEATURES DATAFRAME")
features_df.describe().show()


valid_files = [
    os.path.join("Silver/data/saved_tracks", f)
    for f in os.listdir("Silver/data/saved_tracks")
    if f.endswith(".parquet")
]

print("Silver saved tracks folder data : {}".format(os.listdir(SAVED_TRACKS_PATH)))
saved_tracks_df = spark.read.schema(saved_tracks_schema).parquet(*valid_files)

print("SAVED TRACKS DATAFRAME")
saved_tracks_df.describe().show()

# Normalize the join keys: lowercase & trimmed
features_df = features_df.withColumn("join_name", F.lower(F.trim("name"))) \
                         .withColumn("join_artist", F.lower(F.trim("artists")))

saved_tracks_df = saved_tracks_df.withColumn("join_name", F.lower(F.trim("title"))) \
                                 .withColumn("join_artist", F.lower(F.trim("artist")))

# Remove duplicate entries in saved tracks
saved_tracks_df = saved_tracks_df.dropDuplicates(["join_name", "join_artist"])

print("features_df NORMALIZED")
features_df.describe().show()

print("saved_tracks_df NORMALIZED")
saved_tracks_df.describe().show()

# Join both datasets, keeping all records
joined_df = features_df.join(
    saved_tracks_df,
    on=["join_name", "join_artist"],
    how="full_outer"
)

# Create a Popularity and Energy Mart
popularity_and_energy_mart = joined_df.select(
    "join_name", "join_artist", "popularity", "energy", "valence", "danceability", "release_year"
)

popularity_and_energy_mart.describe().show()

Helpers.write_to_mysql(popularity_and_energy_mart, "popularity_and_energy_mart", "Spotify")


# Audio Feature Summary by Artist
print("NORMAL JOINED DF")
joined_df.select("join_artist", "energy", "valence", "danceability", "tempo", "acousticness", "speechiness").show()
print("DROPNA JOINED DF")
joined_df.select("join_artist", "energy", "valence", "danceability", "tempo", "acousticness", "speechiness").na.drop().show()

audio_summary = joined_df.groupBy("join_artist").agg(
    avg("energy").alias("avg_energy"),
    avg("valence").alias("avg_valence"),
    avg("danceability").alias("avg_danceability"),
    avg("tempo").alias("avg_tempo"),
    avg("acousticness").alias("avg_acousticness"),
    avg("speechiness").alias("avg_speechiness")
)

audio_summary.describe().show()

Helpers.write_to_mysql(audio_summary, "audio_features_summary", "Spotify")


# Evolution of trends over time
trends_mart = joined_df.groupBy("release_year").agg(
    avg("tempo").alias("avg_tempo"),
    avg("loudness").alias("avg_loudness"),
    avg("energy").alias("avg_energy"),
    avg("popularity").alias("avg_popularity"),
    avg("duration_ms").alias("avg_duration_ms")
)

trends_mart.describe().show()

Helpers.write_to_mysql(trends_mart, "trends_evolution", "Spotify")

# Correlation between the length of a track and its popularity
duration_to_popularity_mart = joined_df.select(
    "join_name", "duration_ms", "popularity", "release_year", "tempo"
)

duration_to_popularity_mart.describe().show()

Helpers.write_to_mysql(duration_to_popularity_mart, "duration_to_popularity", "Spotify")



#def count_artists(artists_str):
#    try:
#        artists = ast.literal_eval(artists_str)
#        return len(artists)
#    except:
#        return 1
#
#def is_featuring(artists_str):
#    try:
#        artists = ast.literal_eval(artists_str)
#        return len(artists) > 1
#    except:
#        return False

#count_artists_udf = udf(count_artists, IntegerType())
#is_featuring_udf = udf(is_featuring, BooleanType())

# Vérifier si le répertoire existe et contient des fichiers
#print(os.path.exists(MUSIC_INFO_PATH))
#print("Contenu du répertoire Silver :")
#print(os.listdir(MUSIC_INFO_PATH))
#
#music_info_df = spark.read.parquet(MUSIC_INFO_PATH)
#music_info_df.show(5)
#music_info_df.printSchema()
#
#df_gold = music_info_df.withColumn("nb_artistes", len(col("artists"))) \
#                       .withColumn("featuring", len(col("artist")) > 1)

#df_gold = df.withColumn("nb_artistes", count_artists_udf(col("artists"))) \
#            .withColumn("featuring", is_featuring_udf(col("artists")))

#df_gold.show()

#df_gold.select("artists", "nb_artistes", "featuring").show(10, truncate=False)
#
#df_gold.write.mode("overwrite").option("header", "true").csv(OUTPUT_FILE)
#print(f"Données gold sauvegardées dans {OUTPUT_FILE} avec les colonnes nb_artistes et featuring.")

#query.awaitTermination()