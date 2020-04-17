from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sn
import sqlalchemy

username = "krmiddlebrook"
password = "Transit13"
CONNECTION_STRING = f"dbname='bsdsclass' user='{username}' host='bsds200.c3ogcwmqzllz.us-east-1.rds.amazonaws.com' password='{password}'"  # noqa: E501
db_url = f"postgresql://{username}:{password}@bsds200.c3ogcwmqzllz.us-east-1.rds.amazonaws.com/bsdsclass"  # noqa: E501
schema_name = "song_pop"
engine = sqlalchemy.create_engine(db_url)


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


def plot_track_follower_relationship(base_dir):
    """Plots a scatter plot to show the relationship of track count and follower
    counts as artists reach our threshold of being "popular".

    Uses sql to gather the number of followers and tracks per artist, then
    removes artists who have greater than 200 tracks. We then calculate the 85th,
    90th and 95th percentiles for follwer counts. The 90th percentile is our
    threshold for what makes an artist "popular", and the other two percentiles
    are used to subset our data to seek insight of artists that are approaching
    popularity. Next, we use that subset to plot a scatter plot.

    Args:
        base_dir (path): The base directory you want the plot to be saved to.

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
                    GROUP BY artist_id) as lhs
                INNER JOIN
                    (SELECT artist_id, SUM(track_count) as total_tracks
                    FROM song_pop.albums
                    GROUP BY artist_id) as rhs
                ON lhs.artist_id = rhs.artist_id
                WHERE total_tracks <= 200;
                """
        data = pd.read_sql(query, conn)

    # Artists above the 90th Percentile is defined as popular
    threshold = data.quantile(q=0.9)["follower_count"]

    # Getting the 85th and 95th Percentile
    pct_85 = data.quantile(q=0.85)["follower_count"]
    pct_95 = data.quantile(q=0.95)["follower_count"]

    # Getting all artists who are between
    # the 85th and 95th Percentile of Follower Counts
    filter_boolean = (data.follower_count >= pct_85) & (data.follower_count <= pct_95)

    # Getting X and Y values for our scatter plot
    x = data.total_tracks[filter_boolean]
    y = data.follower_count[filter_boolean]

    # Plotting the Scatter Plot
    plt.clf()
    plt.scatter(x, y)
    plt.plot(
        [0, 200],
        [threshold, threshold],
        "r",
        label="Popularity Threshold = {}".format(int(threshold)),
    )
    plt.title("Artists within 85-95 Percentiles for Follower Count")
    plt.xlabel("Total Tracks")
    plt.ylabel("Follower Count")
    plt.legend()

    # Saving the file
    filename = plot_dir / "follower_track_relationship.png"
    plt.savefig(filename, bbox_inches="tight")

    print_statement = f"""
        Scatter Plot showing the relationship of follower and track counts of
        artists is saved to {filename}.

    """
    print(print_statement)


if __name__ == "__main__":
    base_dir = Path().cwd()

    # plot correlation matrix between artist Spotify followers and Twitter metric
    plot_twitter_influence(base_dir)
    plot_audio_influence(base_dir)
    plot_track_follower_relationship(base_dir)
