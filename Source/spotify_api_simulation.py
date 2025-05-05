import json
import random

NB_TRACKS = 10  # nbr de morceaux à simuler

def simulate_audio_features(track_ids):
    features = []
    for track_id in track_ids:
        features.append({
            "danceability": round(random.uniform(0, 1), 3),
            "energy": round(random.uniform(0, 1), 3),
            "key": random.randint(0, 11),
            "loudness": round(random.uniform(-60, 0), 2),
            "mode": random.randint(0, 1),
            "speechiness": round(random.uniform(0, 1), 3),
            "acousticness": round(random.uniform(0, 1), 3),
            "instrumentalness": round(random.uniform(0, 1), 3),
            "liveness": round(random.uniform(0, 1), 3),
            "valence": round(random.uniform(0, 1), 3),
            "tempo": round(random.uniform(60, 200), 2),
            "id": track_id,
            "uri": f"spotify:track:{track_id}",
            "duration_ms": random.randint(120000, 300000),
            "time_signature": random.randint(3, 4)
        })
    return {"audio_features": features}

def simulate_saved_tracks(n=5):
    tracks = []
    for i in range(n):
        track_id = f"track_{i+1}"
        tracks.append({
            "added_at": "2024-05-04T12:00:00Z",
            "track": {
                "id": track_id,
                "name": f"Track Name {i+1}",
                "artists": [{"name": f"Artist {i+1}"}],
                "album": {"name": f"Album {i+1}"},
                "duration_ms": random.randint(120000, 300000),
                "uri": f"spotify:track:{track_id}"
            }
        })
    return {"items": tracks}

if __name__ == "__main__":
    # simule l'appel de /audio-features : https://developer.spotify.com/documentation/web-api/reference/get-several-audio-features
    track_ids = [f"track_{i+1}" for i in range(NB_TRACKS)]
    audio_features = simulate_audio_features(track_ids)
    with open("simulated_audio_features.json", "w") as f:
        json.dump(audio_features, f, indent=2)
    print("Fichier simulated_audio_features.json généré.")

    # simule l'appel de /me/tracks : https://developer.spotify.com/documentation/web-api/reference/get-users-saved-tracks
    saved_tracks = simulate_saved_tracks(NB_TRACKS)
    with open("simulated_saved_tracks.json", "w") as f:
        json.dump(saved_tracks, f, indent=2)
    print("Fichier simulated_saved_tracks.json généré.")