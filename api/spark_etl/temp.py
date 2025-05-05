from pyspark.sql import SparkSession
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os
import glob
import shutil
from datetime import date

os.environ["HADOOP_HOME"] = r"C:\hadoop-2.8.1"
os.environ["SPARK_LOCAL_DIRS"] = r"C:\tmp"  
os.environ["JAVA_HOME"] = r"C:\Users\eleah\jdk-11.0.27+6"  
os.environ["HADOOP_OPTIONAL_TOOLS"] = "false"


# ------------------- CONFIG -------------------
client_id = "dde733b94d0a407a9b697b92ba4e972b"
client_secret = "52be2c5824d749d4b3db1ec698e608e9"
redirect_uri = "http://127.0.0.1:8888/callback"
scope = "user-library-read"

base_dir = r"C:\Users\eleah\EFREI-M1-Big-Data-Programming-1-TP-Final\Source\stream"
today = date.today().isoformat()
final_csv_path = os.path.join(base_dir, f"saved_tracks_{today}.csv")
temp_dir = os.path.join(base_dir, "temp")

# ------------------- SPOTIFY AUTH -------------------
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=client_id,
    client_secret=client_secret,
    redirect_uri=redirect_uri,
    scope=scope
))

# ------------------- SPARK INIT -------------------
spark = SparkSession.builder \
    .appName("SpotifyDailyCSV") \
    .config("spark.python.worker.reuse", "true") \
    .getOrCreate()

# ------------------- FETCH TRACKS -------------------
def get_saved_tracks():
    tracks = []
    results = sp.current_user_saved_tracks(limit=50)
    while results:
        for item in results['items']:
            track = item['track']
            tracks.append({
                "Title": track["name"],
                "Artist": track["artists"][0]["name"],
                "Album": track["album"]["name"],
                "Spotify ID": track["id"]
            })
        results = sp.next(results) if results['next'] else None
    return tracks

# ------------------- MAIN -------------------
if __name__ == "__main__":
    print(f"📦 Fetching saved tracks for {today}...")

    tracks_data = get_saved_tracks()
    df = spark.createDataFrame(tracks_data)

    # Clean temp dir if exists
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    # Write to Spark temp output
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_dir)

    # Get generated CSV part file
    part_file = glob.glob(os.path.join(temp_dir, "part-*.csv"))[0]

    # Move and rename
    shutil.move(part_file, final_csv_path)
    shutil.rmtree(temp_dir)

    print(f"✅ Done. CSV saved to: {final_csv_path}")
