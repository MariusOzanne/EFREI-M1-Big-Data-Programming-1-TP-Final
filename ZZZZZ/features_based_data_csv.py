import time
import os
from datetime import datetime
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Configuration
STREAM_DIR = "Source/stream"
FEATURES_FILE = "Source/data.csv"
OUTPUT_DIR = "Source/stream_features"

# Créer le dossier de sortie s'il n'existe pas
os.makedirs(OUTPUT_DIR, exist_ok=True)

class NewFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith(".csv"):
            return

        print(f"🎵 Nouveau fichier détecté : {event.src_path}")

        try:
            # Attendre que le fichier soit complètement écrit
            time.sleep(2)

            # Vérifier que le fichier n'est pas vide
            if os.path.getsize(event.src_path) == 0:
                print(f"⚠️ Le fichier {event.src_path} est vide. Ignoré.")
                return

            # Charger les CSV
            saved_tracks = pd.read_csv(event.src_path)
            data = pd.read_csv(FEATURES_FILE)

            # Harmoniser les noms de colonnes
            saved_tracks.rename(columns={"Spotify ID": "id"}, inplace=True)

            # Join sur la colonne 'id'
            merged = pd.merge(saved_tracks, data, on="id", how="inner")

            # Sauvegarder le fichier résultant avec un timestamp
            timestamp = datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
            output_file = os.path.join(OUTPUT_DIR, f"saved_tracks_features_{timestamp}.csv")
            merged.to_csv(output_file, index=False)

            print(f"Fichier traité et sauvegardé : {output_file}")

        except Exception as e:
            print(f"Erreur lors du traitement du fichier {event.src_path} : {e}")

if __name__ == "__main__":
    print(f"👀 Surveillance du dossier : {STREAM_DIR}")
    event_handler = NewFileHandler()
    observer = Observer()
    observer.schedule(event_handler, path=STREAM_DIR, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
