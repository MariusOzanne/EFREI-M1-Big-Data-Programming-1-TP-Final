import csv
import os
import spotipy
from spotipy.oauth2 import SpotifyOAuth

# Spotify API credentials
client_id = "dde733b94d0a407a9b697b92ba4e972b"
client_secret = "52be2c5824d749d4b3db1ec698e608e9"
redirect_uri = "http://127.0.0.1:8888/callback"
scope = "user-library-read"

csv_input = "saved_tracks.csv"
csv_output = "audio_features.csv"

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=client_id,
    client_secret=client_secret,
    redirect_uri=redirect_uri,
    scope=scope
))

def load_spotify_ids(filename):
    if not os.path.exists(filename):
        print(f"File not found: {filename}")
        return []

    with open(filename, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        return [row["Spotify ID"] for row in reader if row["Spotify ID"]]

def fetch_audio_features(track_ids):
    features = []
    batch_size = 100
    for i in range(0, len(track_ids), batch_size):
        batch = track_ids[i:i + batch_size]
        try:
            result = sp.audio_features(batch)
            features.extend([f for f in result if f])  # filter out None results
        except spotipy.SpotifyException as e:
            print(f"Error fetching batch starting at index {i}: {e}")
    return features

def save_audio_features_to_csv(features, filename):
    if not features:
        print("No audio features to save.")
        return

    keys = features[0].keys()
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=keys)
        writer.writeheader()
        for feature in features:
            if feature:  
                writer.writerow(feature)

    print(f"Audio features saved to {filename}")

if __name__ == "__main__":
    ids = load_spotify_ids(csv_input)
    if not ids:
        print("No Spotify IDs found.")
    else:
        print(f"Fetching audio features for 100 tracks...")
        features = fetch_audio_features(ids)
        save_audio_features_to_csv(features, csv_output)
