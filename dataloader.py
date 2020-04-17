"""
authors: Kai Middlebrook, Adam Villarreal, Brian Lopez

depending on your environment you may need to install:
pandas>=1.0.3
numpy>=1.18
sqlalchemy>=1.3
tqdm>=4.44
psycopg2>=2.8
"""
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
import sqlalchemy
from tqdm import tqdm

username = "krmiddlebrook"
password = "Transit13"
CONNECTION_STRING = f"dbname='bsdsclass' user='{username}' host='bsds200.c3ogcwmqzllz.us-east-1.rds.amazonaws.com' password='{password}'"  # noqa: E501
db_url = f"postgresql://{username}:{password}@bsds200.c3ogcwmqzllz.us-east-1.rds.amazonaws.com/bsdsclass"  # noqa: E501
schema_name = "song_pop"


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

    # connect to database server
    db = sqlalchemy.create_engine(db_url)

    # upload data to database
    ## WARNING: the albums table is 3+ million rows. It takes a long time to upload (~30 minutes)! # noqa: E501
    print(
        "Uploading data to database. The data is large so this will take a while..."
    )  # noqa: E501
    bar = tqdm(total=len(all_dfs.keys()), desc="Uploading data to database...")
    for key in all_dfs.keys():
        bar.set_description(f"Uploading {key} to database...")
        with db.connect() as conn:
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
    # connect to database server
    db = sqlalchemy.create_engine(db_url)

    # execute query
    with db.connect() as conn:
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


if __name__ == "__main__":
    base_dir = Path().cwd()

    # all_dfs = load_pandas(base_dir)
    load_sql(base_dir)
    unit_tests(base_dir)
