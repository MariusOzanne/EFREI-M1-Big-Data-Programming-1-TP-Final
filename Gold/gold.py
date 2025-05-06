from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import BooleanType, IntegerType
import ast
import os

spark = SparkSession.builder \
    .appName("SilverToGold") \
    .getOrCreate()

SILVER_PATH = "../Silver/data/musicInfo"
GOLD_PATH = "../gold_data.csv"

def count_artists(artists_str):
    try:
        artists = ast.literal_eval(artists_str)
        return len(artists)
    except:
        return 1

def is_featuring(artists_str):
    try:
        artists = ast.literal_eval(artists_str)
        return len(artists) > 1
    except:
        return False

count_artists_udf = udf(count_artists, IntegerType())
is_featuring_udf = udf(is_featuring, BooleanType())

# Vérifier si le répertoire existe et contient des fichiers
print(os.path.exists(SILVER_PATH))
print("Contenu du répertoire Silver :")
print(os.listdir(SILVER_PATH))

df = spark.read.parquet(SILVER_PATH)
df.show(5)
df.printSchema()

df_gold = df.withColumn("nb_artistes", count_artists_udf(col("artists"))) \
            .withColumn("featuring", is_featuring_udf(col("artists")))

df_gold.show()

df_gold.select("artists", "nb_artistes", "featuring").show(10, truncate=False)

df_gold.write.mode("overwrite").option("header", "true").csv(GOLD_PATH)
print(f"Données gold sauvegardées dans {GOLD_PATH} avec les colonnes nb_artistes et featuring.")

#query.awaitTermination()