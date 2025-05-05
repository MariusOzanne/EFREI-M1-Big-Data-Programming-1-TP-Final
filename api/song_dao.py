from typing import List
from sqlmodel import Session, select
from .song_model import Song
from .db import engine

def dao_get_all_songs() -> List[Song]:
    with Session(engine) as session:
        statement = select(Song)
        songs= session.exec(statement).all()
        return songs
    
def dao_save_songs(songs: List[Song]):
    with Session(engine) as session:
        for song in songs:
            existing_song= session.exec(
                select(Song).where(Song.spotify_id == song.spotify_id)).first()
            if not existing_song:
                session.add(song)
        session.commit()