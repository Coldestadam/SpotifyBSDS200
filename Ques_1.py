"""
authors: Adam Villarreal

depending on your environment you may need to install:
pandas>=1.0.3
numpy>=1.18
sqlalchemy>=1.3
tqdm>=4.44
psycopg2>=2.8
matplotlib>=3.1
seaborn==0.9
"""

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sqlalchemy

# Connecting to the Database Server
username = "krmiddlebrook"
password = "Transit13"
CONNECTION_STRING = f"dbname='bsdsclass' user='{username}' host='bsds200.c3ogcwmqzllz.us-east-1.rds.amazonaws.com' password='{password}'"  # noqa: E501
db_url = f"postgresql://{username}:{password}@bsds200.c3ogcwmqzllz.us-east-1.rds.amazonaws.com/bsdsclass"  # noqa: E501
schema_name = "song_pop"
engine = sqlalchemy.create_engine(db_url)


def get_names(data, num_tracks, top_num):
    subset = data.loc[data.total_tracks == num_tracks, :]
    names = subset.nlargest(top_num, "follower_count").loc[
        :, ["artist_name", "follower_count", "instagram"]
    ]
    return names


def main():
    query = f"""
	        SELECT lhs2.artist_id, follower_count, total_tracks, artist_name, instagram
	        FROM (
	            SELECT lhs.artist_id, follower_count, total_tracks, instagram
	            FROM (
	                SELECT artist_id, MAX(follower_count) as follower_count, MAX(instagram) as instagram
	                FROM song_pop.artist_socials
	                WHERE username in (SELECT DISTINCT username FROM song_pop.twitter)
	                GROUP BY artist_id) as lhs
	            INNER JOIN
	                (SELECT artist_id, SUM(track_count) as total_tracks
	                FROM song_pop.albums
	                GROUP BY artist_id) as rhs
	            ON lhs.artist_id = rhs.artist_id
	            WHERE total_tracks <= 500) as lhs2
	        INNER JOIN
	            (SELECT DISTINCT(artist_id), artist_name FROM song_pop.tracks) as rhs2
	        ON lhs2.artist_id = rhs2.artist_id;"""

    # Getting data from SQL Query
    with engine.connect() as con:
        data = pd.read_sql(query, con)

        # Adding categorical bins for the number of tracks
    new_row = np.empty(data.shape[0])
    new_row[:] = np.nan  # A numpy array full of Nans

    data.loc[:, "track_bins"] = new_row

    # Categoring the Data
    data.loc[data.total_tracks > 200, "track_bins"] = "200+"
    data.loc[data.total_tracks <= 200, "track_bins"] = "176-200"
    data.loc[data.total_tracks <= 175, "track_bins"] = "151-175"
    data.loc[data.total_tracks <= 150, "track_bins"] = "126-150"
    data.loc[data.total_tracks <= 125, "track_bins"] = "101-125"
    data.loc[data.total_tracks <= 100, "track_bins"] = "76-100"
    data.loc[data.total_tracks <= 75, "track_bins"] = "51-75"
    data.loc[data.total_tracks <= 50, "track_bins"] = "26-50"
    data.loc[data.total_tracks <= 25, "track_bins"] = "1-25"

    print("\nThe distribution of artists by category:")
    print(data.track_bins.value_counts())

    # Getting the threshold of what makes a person popular
    popular_threshold = data.follower_count.quantile(0.9)
    print("\nPopularity threshold:", popular_threshold)

    # Subsetting the data to get only the data of popular artists
    popular_data = data.loc[data.follower_count > popular_threshold, :]

    # Getting the Avg Follower Count for each bin
    avg_table = (
        popular_data.groupby("track_bins").agg({"follower_count": "mean"}).round()
    )
    avg_table.columns = ["Avg Followers"]
    avg_table = avg_table.reindex(
        [
            "200+",
            "176-200",
            "151-175",
            "126-150",
            "101-125",
            "76-100",
            "51-75",
            "26-50",
            "1-25",
        ]
    )

    print("\nTable of Average Follower Count for each bin:")
    print(avg_table)

    top_5_1 = get_names(data, num_tracks=1, top_num=5)
    print("\nTop 5 Artists with 1 track:")
    print(top_5_1)
    message = f"""IMPORTANT: There are many artists with 1 track, however we lost a lot of data in our query. You are invited for further investigation"""

    print(message)


if __name__ == "__main__":
    main()
