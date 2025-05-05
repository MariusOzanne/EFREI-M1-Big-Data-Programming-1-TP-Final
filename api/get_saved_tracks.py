import os
import csv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from song_model import Song
from db import create_tables
from typing import List
from datetime import datetime

# Spotify API credentials
client_id = "dde733b94d0a407a9b697b92ba4e972b"
client_secret = "52be2c5824d749d4b3db1ec698e608e9"
redirect_uri = "http://127.0.0.1:8888/callback"
scope = "user-library-read"

# Dynamic filename
today = datetime.today().strftime('%Y-%m-%d')
csv_filename = f"Source/stream/saved_tracks_{today}.csv"  # You can change folder as needed

# Initialize Spotipy
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=client_id,
    client_secret=client_secret,
    redirect_uri=redirect_uri,
    scope=scope
))

def get_saved_tracks() -> List[Song]:
    saved_tracks = []
    results = sp.current_user_saved_tracks(limit=50)

    while results:
        for item in results['items']:
            track = item['track']
            song = Song(
                title=track['name'],
                artist=track['artists'][0]['name'],
                album=track['album']['name'],
                spotify_id=track['id']
            )
            saved_tracks.append(song)

        if results['next']:
            results = sp.next(results)
        else:
            break

    return saved_tracks

def load_existing_ids_from_csv(filename: str) -> set:
    if not os.path.exists(filename):
        return set()
    
    with open(filename, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        return {row["Spotify ID"] for row in reader}

def export_to_csv(songs: List[Song], filename: str):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Title", "Artist", "Album", "Spotify ID"])
        for song in songs:
            writer.writerow([song.title, song.artist, song.album, song.spotify_id])
    print(f"CSV updated: {filename} ({len(songs)} songs)")

if __name__ == "__main__":
    create_tables()
    print("Checking for new saved songs...")

    existing_ids = load_existing_ids_from_csv(csv_filename)
    current_songs = get_saved_tracks()
    current_ids = {song.spotify_id for song in current_songs}

    if current_ids != existing_ids:
        export_to_csv(current_songs, csv_filename)
    else:
        print("No new songs. CSV is up to date.")
