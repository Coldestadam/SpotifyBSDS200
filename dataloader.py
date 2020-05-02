"""
authors: Kai Middlebrook, Adam Villarreal, Brian Lopez

depending on your environment you may need to install:
pandas>=1.0.3
numpy>=1.18
sqlalchemy>=1.3
tqdm>=4.44
psycopg2>=2.8
matplotlib>=3.1
seaborn==0.9
"""
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import psycopg2
import seaborn as sn
import sqlalchemy
from tqdm import tqdm

username = "krmiddlebrook"
password = "Transit13"
CONNECTION_STRING = f"dbname='bsdsclass' user='{username}' host='bsds200.c3ogcwmqzllz.us-east-1.rds.amazonaws.com' password='{password}'"  # noqa: E501
db_url = f"postgresql://{username}:{password}@bsds200.c3ogcwmqzllz.us-east-1.rds.amazonaws.com/bsdsclass"  # noqa: E501
schema_name = "song_pop"
engine = sqlalchemy.create_engine(db_url)  # connect to database server


def load_data(base_dir):
    """Loads the data from GCS if it doesn't exist locally and saves it to disk.

    Args:
        base_dir (path): The base directory where you want to store the data.

    Returns:
        all_dfs (dict): a dict containing
            {'albums': albums_df,
            'artist_socials': artist_socials_df,
            'twitter': twitter_df}
    """

    # urls to pointing to the GCS bucket where the data lives
    albums_url = "https://storage.googleapis.com/song_pop/albums.parquet"
    # TODO: replace local path to GCS path
    artists_socials = "https://storage.cloud.google.com/song_pop/artist_socials.parquet"
    twitter_url = "https://storage.googleapis.com/song_pop/twitter.parquet"

    # set the data directory where the files will be stored locally
    data_dir = Path(base_dir) / "data"
    # make the directory if it doesn't exist'
    data_dir.mkdir(parents=True, exist_ok=True)

    # valid data filenames and links to download them if necessary
    valid_files = {
        "albums": albums_url,
        "artist_socials": artists_socials,
        "twitter": twitter_url,
    }

    already_downloaded = [p.stem for p in Path(data_dir).glob("*.tdf")]
    # download data if it doesn't exist locally
    for file in tqdm(valid_files.keys(), desc="Downloading files..."):
        if file not in already_downloaded:
            # read data from GCS bucket
            df = pd.read_parquet(valid_files[file])
            df = df.convert_dtypes()

            # write data to disk
            df.to_csv((data_dir / (file + ".tdf")), sep="\t", index=False)

    # retrieve local data files
    all_dfs = {}
    for file in tqdm(
        valid_files.keys(), desc="Reading data from disk...this may take a minute"
    ):
        df = pd.read_csv((data_dir / (file + ".tdf")), sep="\t", engine="python")
        df = df.convert_dtypes()
        all_dfs[file] = df

    return all_dfs


def load_pandas(base_dir):
    # retrieve the data files, downloading them if necessary
    all_dfs = load_data(base_dir)
    return all_dfs


def load_sql(base_dir):
    """Uploads local/remote data files to our SQL database.

    Retrieves the local/remote data files and stores them in pandas dataframes.
    Then we upload large dataframes to the database in chunks.

    Args:
        base_dir (path): The base directory where you want to store the data.

    """

    # load/retrieve data from disk
    all_dfs = load_pandas(base_dir)

    # upload data to database
    ## WARNING: the albums table is 3+ million rows. It takes a long time to upload (~30 minutes)! # noqa: E501
    print(
        "Uploading data to database. The data is large so this will take a while..."
    )  # noqa: E501
    bar = tqdm(total=len(all_dfs.keys()), desc="Uploading data to database...")
    for key in all_dfs.keys():
        bar.set_description(f"Uploading {key} to database...")
        with engine.connect() as conn:
            # if the data is more than 100k rows upload data in chunks of 1k
            num_rows = all_dfs[key].shape[0]
            if num_rows > 50000:
                chunk_size = 10000
                print(f"Data is large! Uploading in chunks of size: {chunk_size}.")
                chunk_bar = tqdm(total=num_rows, desc="uploading chunks")
                for i in range(0, num_rows, chunk_size):
                    end = (
                        (i + chunk_size - 1)
                        if (i + chunk_size - 1) < num_rows
                        else None
                    )
                    # drop table if this is the first chunk else append to existing table # noqa: E501
                    all_dfs[key].loc[i:end, :].to_sql(
                        name=key,
                        con=conn,
                        schema=schema_name,
                        index=False,
                        if_exists="append" if i > 0 else "replace",
                        method="multi",
                    )
                    chunk_bar.update(chunk_size)
                chunk_bar.close()
            else:
                all_dfs[key].to_sql(
                    name=key,
                    con=conn,
                    schema=schema_name,
                    index=False,
                    if_exists="replace",
                    method="multi",
                )
        bar.update(1)
    bar.close()

    # grant universal access to the upload tables
    for key in all_dfs.keys():
        grant_universal_access(table_name=key, schema=schema_name)

    print("Data uploaded to database successfully!")


def grant_universal_access(table_name, schema):
    """Function to grant universal access to a table in our database.

    Args:
        table_name (str): A valid table name in the database
        schema (str): A valid schema where the table is located in the database
    """

    # make sure args are strings
    assert isinstance(table_name, str) is True
    assert isinstance(schema, str) is True

    # create the query to grant universal access
    query = f"""
            GRANT ALL on {table_name}.{schema} to students;
            """
    # execute query
    with engine.connect() as conn:
        conn.execute(query)


def unit_tests(base_dir):
    """Runs tests to check if the data was uploaded to the database properly.

    Runs a series of test to confirm no data was lost or malformed
    after storing it locally and uploading it to the database.
    Checks that local pandas data and sql database table data match.

    Args:
        base_dir (path): The base directory where the data will be stored/saved.

    Test:
        (1) checks if the number of rows in each sql table are
            equal to the number of rows in each of the pandas dataframes
        (2) checks that count of total rows grouped by year are equal in the albums table/dateframe  # noqa: E501
            checks if the average follower count in the artist_socials table/dataframe are equal # noqa: E501
            checks if the average number of likes in the twitter table/dataframe are equal # noqa: E501
    """
    # retrieve dataframes
    dataframes = load_pandas(base_dir)
    albums = dataframes["albums"]
    artist_socials = dataframes["artist_socials"]
    twitter = dataframes["twitter"]

    # connect to sql server
    SQLConn = psycopg2.connect(CONNECTION_STRING)
    SQLCursor = SQLConn.cursor()

    ## Test 1: Checking the size of the data
    for key in list(dataframes.keys()):
        SQLCursor.execute("""Select count(*) from {}.{};""".format(schema_name, key))
        sql_rows = SQLCursor.fetchall()
        sql_rows = sql_rows[0][0]

        DF_rows = dataframes[key].shape[0]
        try:
            assert DF_rows == sql_rows
        except:
            raise Exception(
                f"Number of rows in local {key} dataframe does not match number of \
                rows in database table! (df vs table rows: {DF_rows}, {sql_rows}"
            )

    ## Test 2: Checking the values of an important columns
    # albums
    SQLCursor.execute(
        """
        SELECT year, COUNT(*) as ct
        FROM song_pop.albums
        GROUP BY 1 ORDER BY ct DESC;
        """
    )
    sql_rows = SQLCursor.fetchall()
    sql_rows = (
        pd.DataFrame(sql_rows, columns=["year", "ct"])
        .sort_values(["year"], ascending=True)
        .reset_index(drop=True)
    )
    DF_rows = (
        albums.year.value_counts()
        .to_frame()
        .reset_index()
        .rename(columns={"year": "ct", "index": "year"})
        .sort_values(["year"], ascending=True)
        .reset_index(drop=True)
    )
    try:
        assert np.sum(np.sum(DF_rows - sql_rows)) == 0
    except:
        raise Exception(
            "Number of rows in the albums dataframe and table grouped by year do not\
             match"
        )

    # artist socials
    # check the AVG follower count in the artists_socials table
    SQLCursor.execute("""SELECT AVG(follower_count) FROM song_pop.artist_socials;""")
    sql_avg = np.round(float(SQLCursor.fetchall()[0][0]))
    # rounding to the nearest int to escape rejection based on float size
    follower_avg = np.round(artist_socials.follower_count.mean())
    try:
        assert sql_avg == follower_avg
    except:
        raise Exception(
            f"Average follower count in the artist_socials dataframe and table do not \
            match ({follower_avg}, {sql_avg})"
        )

    # twitter
    # we are going to see if the average amount of likes are equal in pandas and sql
    SQLCursor.execute("""SELECT AVG(likes) FROM song_pop.twitter;""")
    sql_avg = np.round(float(SQLCursor.fetchall()[0][0]))
    likes_avg = np.round(twitter.likes.mean())
    try:
        assert sql_avg == likes_avg
    except:
        raise Exception(
            f"Average number of likes are not equal in the twitter data and table\
             ({likes_avg}, {sql_avg})"
        )

    print("Passed ALL Tests!!!")


def plot_twitter_influence(base_dir):
    """Plots a correlation matrix between artist follower_counts and twitter
    metrics.

    Uses sql to filter and join the artist_socials and twitter tables from our
    database. Next, we calculate a correlation matrix between the following
    numerical variables: follower_count (Spotify), followers, following, likes,
    tweets, and verified. All varibales except follower_count are from twitter.
    Finally, we plot a heatmap of the covariance matrix and save the plot to a
    local directory.

    Args:
        base_dir (path): The base directory you want the plot to be saved to.

    """

    # make sure the plot directory exists or create it so we can save our plot
    plot_dir = Path(base_dir) / "plot"
    Path(plot_dir).mkdir(parents=True, exist_ok=True)

    # load tables into pandas
    with engine.connect() as conn:
        # filter-join artist_socials and twitter tables
        query = f"""
                select *
                from (
                    select username, follower_count
                    from (SELECT *,
                            row_number() OVER (PARTITION BY artist_id
                                               ORDER BY follower_count DESC) rn
                        FROM song_pop.artist_socials) as innerQ
                    where rn = 1
                        and username is not NULL) as lhs
                join
                    (select username, followers, following, likes, tweets, verified
                    from (select *,
                            row_number() OVER (PARTITION BY id
                                               ORDER BY followers DESC) rn
                          from song_pop.twitter) as innerQ
                    where rn = 1
                        and username is not NULL
                        and followers is not NULL
                        and following is not NULL
                        and likes is not NULL
                        and tweets is not NULL
                        and verified is not NULL) as rhs
                using(username);
                """
        combined = pd.read_sql(query, conn)

    # create correlation matrix
    corrMatrix = combined.loc[
        :, ["follower_count", "followers", "following", "likes", "tweets", "verified"]
    ].corr()

    # make heatmap of the corrMatrix
    heat = sn.heatmap(corrMatrix, annot=True)
    heat.set_title(
        "Correlation Matrix between Spotify Follower Count and Twitter Metrics"
    )
    plt.tight_layout()

    # save heatmap
    filename = plot_dir / "follower_count_twitter_metrics_corr.png"
    plt.savefig(filename)

    print_statement = f"""
        Correlation Matrix Heatmap between Spotify follower_count and Twitter metrics
        saved to: {filename}.
    """
    print(print_statement)


def plot_audio_influence(base_dir):
    """Plots a correlation matrix between artist follower_counts and audio
    characteristics.

    Uses sql to filter and aggregate track audio features grouped by artist to
    a dataframe. Next, we calculate a correlation matrix between the following
    numerical variables: follower_count, avg(danceability), avg(energy),
        avg(loudness), avg(speechiness), avg(acousticness),
        avg(instrumentalness), avg(liveness), avg(valence), avg(tempo),
        avg(time_signature), avg(duration_ms).
    For averaged variables, we group by artist_id to get the average audio
    characteristics of each artist. Finally, we plot a
    heatmap of the covariance matrix and save the plot to a local directory.

    Args:
        base_dir (path): The base directory you want the plot to be saved to.

    """

    # make sure the plot directory exists or create it so we can save our plot
    plot_dir = Path(base_dir) / "plot"
    Path(plot_dir).mkdir(parents=True, exist_ok=True)

    # load tables into pandas
    with engine.connect() as conn:
        # filter-join artist_socials and twitter tables
        query = f"""
                select
                    follower_count, danceability, energy, loudness,
                    speechiness, acousticness, instrumentalness, liveness,
                    valence, tempo, time_signature, duration_ms
                from (
                    select artist_id, avg(danceability) danceability,
                        avg(energy) energy, avg(loudness) loudness,
                        avg(speechiness) speechiness, avg(acousticness) acousticness,
                        avg(instrumentalness) instrumentalness, avg(liveness) liveness,
                        avg(valence) valence, avg(tempo) tempo,
                        avg(time_signature) time_signature, avg(duration_ms) duration_ms
                    from song_pop.tracks
                    group by artist_id) as lhs
                join
                    (select artist_id, max(follower_count) follower_count
                    from song_pop.artist_socials
                    group by artist_id) as rhs
                using(artist_id)
                """
        combined = pd.read_sql(query, conn)

    # create correlation matrix
    corrMatrix = combined.corr()

    # make heatmap of the corrMatrix
    heat = sn.heatmap(corrMatrix)
    heat.set_title(
        "Correlation Matrix between Spotify Follower Count and Audio Characteristics"
    )
    plt.tight_layout()

    # save heatmap
    filename = plot_dir / "follower_count_audio_characteristics_corr.png"
    plt.savefig(filename)

    print_statement = f"""
        Correlation Matrix Heatmap between Spotify follower_count and Audio
        Characteristics saved to: {filename}.
    """
    print(print_statement)


def plot_track_follower_relationship(base_dir, max_track=500):
    """Plots a scatter plot to show the relationship of track count and follower
    counts as artists reach our threshold of being "popular".

    Uses sql to gather the number of followers and tracks per artist, then
    removes artists who have greater than 500 tracks. We then calculate the 85th,
    90th and 95th percentiles for follwer counts. The 90th percentile is our
    threshold for what makes an artist "popular", and the other two percentiles
    are used to subset our data to seek insight of artists that are approaching
    popularity. Next, we use that subset to plot a scatter plot.

    If max_track is used, then we will subset the data to retain the artists who
    have total track counts less than max_track.

    Args:
        base_dir (path): The base directory you want the plot to be saved to.
        max_track (int): The maximum number of track counts an artist can have

    """

    # make sure the plot directory exists or create it so we can save our plot
    plot_dir = Path(base_dir) / "plot"
    Path(plot_dir).mkdir(parents=True, exist_ok=True)

    # load tables into pandas
    with engine.connect() as conn:

        # Joins artist_socials and albums to get data
        query = f"""
                SELECT lhs.artist_id, follower_count, total_tracks
                FROM (
                    SELECT artist_id, MAX(follower_count) as follower_count
                    FROM song_pop.artist_socials
                    WHERE username in (SELECT DISTINCT username FROM song_pop.twitter)
                    GROUP BY artist_id) as lhs
                INNER JOIN
                    (SELECT artist_id, SUM(track_count) as total_tracks
                    FROM song_pop.albums
                    GROUP BY artist_id) as rhs
                ON lhs.artist_id = rhs.artist_id
                WHERE total_tracks <= 500;
                """
        data = pd.read_sql(query, conn)

    # Artists above the 90th Percentile is defined as popular
    threshold = data.quantile(q=0.9)["follower_count"]

    # Getting the 85th and 95th Percentile
    pct_85 = data.quantile(q=0.85)["follower_count"]
    pct_95 = data.quantile(q=0.95)["follower_count"]

    # Getting all artists who are between
    # the 85th and 95th Percentile of Follower Counts
    # And have at most 200 tracks
    filter_boolean = (data.follower_count >= pct_85) & (data.follower_count <= pct_95) & (data.total_tracks <= max_track)

    # Getting X and Y values for our scatter plot
    x = data.total_tracks[filter_boolean]
    y = data.follower_count[filter_boolean]

    # Plotting the Scatter Plot
    plt.clf()
    plt.scatter(x, y)
    plt.plot(
        [0, max_track],
        [threshold, threshold],
        "r",
        label="Popularity Threshold = {}".format(int(threshold)),
    )
    plt.title("Artists within 85-95 Percentiles for Follower Count")
    plt.xlabel("Total Tracks (max_track = {})".format(max_track))
    plt.ylabel("Follower Count")
    plt.legend()

    # Saving the file
    filename = plot_dir / "follower_track_relationship(most{}).png".format(max_track)
    plt.savefig(filename, bbox_inches="tight")

    print_statement = f"""
        Scatter Plot showing the relationship of follower and track counts of
        artists is saved to {filename}.

    """
    print(print_statement)


    #ANSWER TO QUESTION 1
    def get_names(data, num_tracks, top_num):
        subset = data.loc[data.total_tracks==num_tracks, :]
        names = subset.nlargest(top_num, 'follower_count').loc[:, ['artist_name', 'follower_count', 'instagram']]
        return names

    def Ques_1():
        print('This is the answer for Question 1:\n\n')
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

        #Getting data from SQL Query
        with engine.connect() as con:
            data = pd.read_sql(query, con)

        #Adding categorical bins for the number of tracks
        new_row = np.empty(data.shape[0])
        new_row[:] = np.nan#A numpy array full of Nans

        data.loc[:, 'track_bins'] = new_row

        #Categoring the Data
        data.loc[data.total_tracks>200, 'track_bins'] = '200+'
        data.loc[data.total_tracks<=200, 'track_bins'] = '176-200'
        data.loc[data.total_tracks<=175, 'track_bins'] = '151-175'
        data.loc[data.total_tracks<=150, 'track_bins'] = '126-150'
        data.loc[data.total_tracks<=125, 'track_bins'] = '101-125'
        data.loc[data.total_tracks<=100, 'track_bins'] = '76-100'
        data.loc[data.total_tracks<=75, 'track_bins'] = '51-75'
        data.loc[data.total_tracks<=50, 'track_bins'] = '26-50'
        data.loc[data.total_tracks<=25, 'track_bins'] = '1-25'

        print('\nThe distribution of artists by category:')
        print(data.track_bins.value_counts())

        #Getting the threshold of what makes a person popular
        popular_threshold = data.follower_count.quantile(0.9)
        print('\nPopularity threshold:', popular_threshold)

        #Subsetting the data to get only the data of popular artists
        popular_data = data.loc[data.follower_count > popular_threshold, :]

        #Getting the Avg Follower Count for each bin
        avg_table = popular_data.groupby('track_bins').agg({'follower_count':'mean'}).round()
        avg_table.columns = ['Avg Followers']
        avg_table = avg_table.reindex(['200+', '176-200', '151-175', '126-150', 
                                       '101-125', '76-100', '51-75', '26-50', '1-25'])

        print('\nTable of Average Follower Count for each bin:')
        print(avg_table)



        top_5_1 = get_names(data, num_tracks=1, top_num=5)
        print('\nTop 5 Artists with 1 track:')
        print(top_5_1)
        message = f"""IMPORTANT: There are many artists with 1 track, however we lost a lot of data in our query. You are invited for further investigation"""

        print(message)


if __name__ == "__main__":
    base_dir = Path().cwd()

    # dataloading
    load_sql(base_dir)
    unit_tests(base_dir)

    # plotting
    plot_twitter_influence(base_dir)
    plot_audio_influence(base_dir)
    plot_track_follower_relationship(base_dir, max_track=200)
    plot_track_follower_relationship(base_dir, max_track=500)

    #Answers
    Ques_1()
