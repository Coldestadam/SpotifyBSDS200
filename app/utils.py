"""
authors: Kai Middlebrook
"""

from datetime import datetime

import spotipy  # noqa: F401
from config import SPOTIPY_CLIENTS, SPOTIPY_CREDS, SPOTIPY_REQUESTS_TIMEOUT  # noqa
from dsjobs import app
from spotipy.oauth2 import SpotifyClientCredentials  # noqa: F401


def chunkify(input, chunk_size=20):
    """
    A convenience function to chunk a list

    """
    chunked = []
    for i in range(0, len(input), chunk_size):
        end = i + chunk_size if i + chunk_size < len(input) else len(input)
        chunked.append(list(input[i:end]))
    return chunked


class SpotipyMux:
    def __init__(self, starting_point=0):
        self.iter_count = starting_point

    def client(self):
        if self.iter_count >= len(SPOTIPY_CLIENTS):
            self.iter_count -= len(SPOTIPY_CLIENTS) + 1
        self.iter_count += 1
        return SPOTIPY_CLIENTS[self.iter_count % len(SPOTIPY_CLIENTS)]

    def token(self):
        return self.client().client_credentials_manager.get_access_token()


@app.task(bind=True)
def flow_complete(self, task_name, time):
    total_time = (
        datetime.utcnow() - datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%f")
    ).total_seconds()  # noqa: E501
    return {"task_name": task_name, "elapsed_time": total_time}
