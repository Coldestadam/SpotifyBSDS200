"""
authors: Kai Middlebrook
"""

import json
import os

# spotipy imports
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

# sqlalchemy imports
from sqlalchemy import create_engine

# local imports

""" DB config """
engine = create_engine(os.getenv("DB_URL"), echo=False, max_overflow=40)
db_name = os.getenv("DB_NAME")
db_schema = os.getenv("DB_SCHEMA")
db_username = os.getenv("DB_USERNAME")
db_host = os.getenv("DB_HOST")


""" Spotify config """
# Spotify Credentials for Spotify API (used with Spotipy)
with open("app/spotify_keys.json", "r") as fp:
    SPOTIPY_CREDS = json.load(fp)
SPOTIPY_REQUESTS_TIMEOUT = 15
SPOTIPY_CLIENTS = []
for creds in SPOTIPY_CREDS:
    cm = SpotifyClientCredentials(
        client_id=creds.get("ID", None),
        client_secret=creds.get("SECRET", None),  # noqa: E501
    )
    SPOTIPY_CLIENTS.append(
        spotipy.Spotify(
            client_credentials_manager=cm, requests_timeout=SPOTIPY_REQUESTS_TIMEOUT,
        )
    )


# table names
tables = {
    "albums": "albums",
    "artist_socials": "artist_socials",
    "twitter": "twitter",
    "tracks": "tracks",
}
