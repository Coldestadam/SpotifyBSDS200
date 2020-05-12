"""
authors: Kai Middlebrook
"""

import random
import time
from datetime import datetime

import pandas as pd
import spotipy
from celery import group
from celery.utils.log import get_task_logger

import config as config
from dsjobs import app
from utils import SpotipyMux, chunkify, flow_complete

logger = get_task_logger(__name__)


@app.task(bind=True)
def get_tracks(self):
    """Collects track metadata from spotify and uploads it to our database.

    Retreives a list of album ids from the "albums" table in our database. Then,
    we splits this list into chunks and assigns celery tasks to each one. This
    allows our program to distribute (e.g., multi-process) tasks across a set of
    workers rather than needing to completing each chunk and their associated
    tasks in sequential order.

    """

    time = datetime.utcnow()
    time = time.replace(minute=0, second=0, microsecond=0)

    query = f"""
        SELECT DISTINCT album_id
        FROM (
            SELECT album_id, max(year) as year, max(track_count) as track_count
            FROM {config.db_schema}.{config.tables.get('albums')}
            WHERE track_count < 23 and year < 2020
            GROUP BY album_id
        ORDER BY year DESC, track_count DESC) as innerQ
        LIMIT 200000;
    """
    with config.engine.connect() as conn:
        album_ids = pd.read_sql(query, con=conn).album_id.values.tolist()

    # split the album ids list into chunks to create more memory efficient tasks
    album_ids = chunkify(album_ids)

    # create the async tasks
    tasks = group(
        (albums_tracks.s(ids, i) | tracks_audio_features.s() | push_tracks.s())
        for i, ids in enumerate(album_ids)
    )

    tracks_flow = (  # noqa: 841
        ((tasks) | drop_dup_tracks.si() | flow_complete.si(self.name, time))
        .delay()
        .get()
    )


@app.task(bind=True, task_time_limit=int(60 * 2))
def albums_tracks(self, album_ids, chunk_number):
    """Collects track info for a set of album ids from Spotify.

    Args:
        album_ids (list): A list of Spotify album ids.

    Returns:
        tracks_list (list): A list of dictionaries containing track metadata
            an audio features.
    """

    spm = SpotipyMux(chunk_number)
    tracks_list = []
    for start in range(0, len(album_ids), 20):
        end = start + 20 if (start + 20) < len(album_ids) else None
        try:
            # request albums from spotify api
            albums = spm.client().albums(album_ids[start:end]).get("albums", [])
            for album in albums:
                # collect album metadata
                if album is not None:
                    album_dict = {
                        "album_id": album.get("id", None),
                        "artist_id": album.get("artists", [{"id": None}])[0].get("id"),
                        "artist_name": album.get("artists", [{"name": None}])[0].get(
                            "name"
                        ),
                        "label": album.get("label", None),
                    }
                    # collect album's track metadata
                    tracks = album.get("tracks", {"items": []}).get("items")
                    for track in tracks:
                        track_dict = {
                            "duration_ms": track.get("duration_ms", None),
                            "explicit": track.get("explicit", None),
                            "track_id": track.get("id", None),
                            "track_name": track.get("name", None),
                            "preview_url": track.get("preview_url", None),
                            "isrc": track.get("external_ids", {"isrc": None}).get(
                                "isrc"
                            ),
                        }
                        track_dict.update(album_dict)
                        tracks_list.append(track_dict)
        except spotipy.client.SpotifyException as e:
            logger.info(f"Spotify ERROR: {e}")
        except Exception as e:
            logger.info(f"ERROR: {e}")

    logger.info(f"tracks collected: {len(tracks_list)}")
    return tracks_list


@app.task(bind=True, task_time_limit=int(60 * 2))
def tracks_audio_features(self, tracks):
    """Collects audio features for a set of tracks from Spotify.

    Retrieves audio features pertaining to each track in the passed in tracks
    list. After this process is complete, the tracks metadata and audio features
    metadata are combined on track_id. Finally, a list containing the combined
    metadata is returned.

    Args:
        tracks (list): A list of dictionaries containing track metadata

    Returns:
        tracks (list): A list of dictionaries containing track metadata and
            audio features
    """

    # create spotipy mux object to distribute request across clients
    spm = SpotipyMux(random.randint(0, 100))
    # create df from tracks input
    tracks = pd.DataFrame(tracks)
    # get all unique track ids
    track_ids = tracks["track_id"].unique().tolist()
    # create list to store audio features metadata
    audio_features = []

    # Loop through track_ids list to collect audio feautres pertaining to
    # each track. Normally, we would loop through the passed in tracks list and
    # update each track's dictionaries with the assocaited audio features
    # metadata. But, this would require making a single API request for each
    # track, making our function much slower. A more efficient way is to
    # include multiple track ids in each api request. But, this requires us to
    # merge the audio features metadata with the tracks metadata. Thus, we
    # create a list of track ids and a list to store the audio features
    # pertaining to the track id.
    for start in range(0, len(track_ids), 50):
        end = start + 50 if (start + 50) < len(track_ids) else None
        try:
            # request audio features from Spotify API
            raw_audio_features = spm.client().audio_features(track_ids[start:end])
            # format metadata for database
            for track in raw_audio_features:
                track = {
                    "danceability": track.get("danceability", None),
                    "energy": track.get("energy", None),
                    "key": track.get("key", None),
                    "loudness": track.get("loudness", None),
                    "mode": track.get("mode", None),
                    "speechiness": track.get("speechiness", None),
                    "acousticness": track.get("acousticness", None),
                    "instrumentalness": track.get("instrumentalness", None),
                    "liveness": track.get("liveness", None),
                    "valence": track.get("valence", None),
                    "tempo": track.get("tempo", None),
                    "track_id": track.get("id", None),
                    "time_signature": track.get("time_signature", None),
                }
                audio_features.append(track)
        except spotipy.client.SpotifyException as e:
            logger.info(f"Spotify ERROR: {e}")
        except Exception as e:
            logger.info(f"ERROR: {e}")

    # combine tracks metadata and audio features metadata
    audio_features = pd.DataFrame(audio_features)
    tracks = tracks.merge(audio_features, on="track_id")

    # removed duplicate tracks containing the same track_id, album_id, and
    # artist_id
    tracks = tracks.drop_duplicates(
        subset=["track_id", "album_id", "artist_id"], ignore_index=True
    )

    # convert dataframe to list so celery can serialize it
    tracks = tracks.to_dict("records")
    return tracks


@app.task(bind=True, task_time_limit=30, queue="postgres")
def push_tracks(self, tracks):
    """Uploads track metadata to the "tracks" table in the database

    Given a list of dictionaries containing audio features and track
    metadata, this function safely uploads this data to the "tracks"
    table in our database.

    Args:
        tracks (list): A list containing dictionaries of audio features
        and track metadata of Spotify tracks.
    """
    # logger.info(f'input: {tracks}')
    if not isinstance(tracks, pd.DataFrame):
        # convert the list to a dataframe
        tracks = pd.DataFrame(tracks)
    tracks = tracks.convert_dtypes()

    # push the tracks data to our database
    with config.engine.connect() as conn:
        tracks.to_sql(
            name=config.tables.get("tracks"),
            con=conn,
            schema=config.db_schema,
            index=False,
            if_exists="append",
            method="multi",
        )
        logger.info(
            f"Tracks data added to database table: {config.tables.get('tracks')}"
        )


@app.task(
    bind=True, task_time_limit=int(60 * 4), queue="postgres",
)
def drop_dup_tracks(self):
    """Deletes duplicate rows in the "tracks" table in our database.

    Duplicate rows are considered table rows that contain the same matching
    track id, album id, and artist id. This function deletes these rows from our
    table--keeping it as lean as possible.

    """

    # sleep for a few seconds in case a some tracks haven't finished uploaded
    # to our db
    time.sleep(5)

    # open a connection to our db
    with config.engine.connect() as conn:
        # Delete all duplicate rows by first creating a CTE to assign
        # a row number to each row that belongs to the set associated with
        # a track_id, album_id, artist_id. In other words, duplicate rows will
        # be assigned a value larger than one when there is more than one item
        # associated to the same track_id, album_id, and artist_id group. Next,
        # we join our CTE table with our "tracks" table by matching on the ctid.
        # Finally, we delete all rows that have a row number greater than 1 from
        # our "tracks" table and close the connection to our db once this is
        # complete.
        query = f"""
            WITH dups AS (
                SELECT ctid,
                   row_number() OVER (PARTITION BY track_id, album_id,
                                                   artist_id
                                      ORDER BY track_id, album_id, artist_id) rn
                   FROM {config.db_schema}.{config.tables.get('tracks')}
            )
            DELETE FROM {config.db_schema}.{config.tables.get('tracks')}
               USING dups
               WHERE dups.rn > 1
                    AND
                     dups.ctid = {config.db_schema}.{config.tables.get('tracks')}.ctid;
        """
        conn.execute(query)
        logger.info(
            f"Removed duplicate rows from: {config.tables.get('tracks')} table."
        )
