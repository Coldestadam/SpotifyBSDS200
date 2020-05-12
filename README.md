## Personal Links

- Brian Lopez
  - Linkedin: [https://www.linkedin.com/in/brianjlopez/](https://www.linkedin.com/in/brianjlopez/)
  - Github: [https://github.com/bjlopez2](https://github.com/bjlopez2)
- Kai Middlebrook
  - Linkedin: [https://www.linkedin.com/in/kaimiddlebrook/](https://www.linkedin.com/in/kaimiddlebrook/)
  - Github: [https://github.com/krmiddlebrook/](https://github.com/krmiddlebrook/)
- Adam Villarreal
  - Linkedin: [https://www.linkedin.com/in/adam-v99/](https://www.linkedin.com/in/adam-v99/)
  - Github: [https://github.com/Coldestadam](https://github.com/Coldestadam)

# Understanding Spotify Popularity

What makes an artist "popular"? Is it luck? Talent? Social media skills? These are questions we investigate in this work.

We determined artist popularity based on their number of Spotify followers. Artists were assigned a popularity label based on whether their follower count was above or below the 90th percentile. Note, we arbitrarily set the popularity threshold to the 90th percentile. Nonetheless, we believe this value is restrictive enough to  merit a valid representation of the true popularity threshold.

# Data Collections
To investigate the artist popularity question, we collected over 3 million data points from both Spotify and Twitter. In particular, track audio characteristics, Twitter user metrics, and artist discographies we gathers. While we could consider other information such as YouTube view counts, we felt these data points were a good place to start.      

### Variables Used
The variables we explored are shown below.

1. Follower Count - The number of Spotify followers for each artist
2. Track Count - The number of tracks that each artists has on Spotify
3. Twitter Followers - The number of Twitter followers per artist
4. Twitter Following - The number of twitter accounts that the artist follows
5. Twitter Likes - The number of Twitter likes that an artist has
6. Twitter Tweets - The numer of tweets (Twitter Posts) that an artist has
7. Twitter Verified - Details whether the artist is verified on Twitter
8. Danceability - A score describing how suitable a track is for dancing based on a combination of musical elements including tempo, rhythm stability, beat strength, and overall regularity. A value of 0.0 is least danceable and 1.0 is most danceable.
9. Energy - A measure from 0.0 to 1.0 that represents a perceptual measure of intensity and activity. Typically, energetic tracks feel fast, loud, and noisy.
10. Loudness - The overall loudness of a track in decibels (dB). Loudness values are averaged across the entire track and are useful for comparing relative loudness of tracks. Loudness is the quality of a sound that is the primary psychological correlate of physical strength (amplitude). Values typical range between -60 and 0 db.
11. Speechiness - A score representing the presence of spoken words in a track. The more exclusively speech-like the recording (e.g. talk show, audio book, poetry), the closer to 1.0 the attribute value. Values above 0.66 describe tracks that are probably made entirely of spoken words. Values between 0.33 and 0.66 describe tracks that may contain both music and speech, either in sections or layered, including such cases as rap music. Values below 0.33 most likely represent music and other non-speech-like tracks
12. Acousticness - A confidence measure from 0.0 to 1.0 of whether the track is acoustic. 1.0 represents high confidence the track is acoustic.
13. Instrumentalness - A prediction value that represents whether a track contains no vocals. “Ooh” and “aah” sounds are treated as instrumental in this context. Rap or spoken word tracks are clearly “vocal”. The closer the instrumentalness value is to 1.0, the greater likelihood the track contains no vocal content. Values above 0.5 are intended to represent instrumental tracks, but confidence is higher as the value approaches 1.0
14. Liveness -	Detects the presence of an audience in the recording. Higher liveness values represent an increased probability that the track was performed live. A value above 0.8 provides strong likelihood that the track is live.
15. Valence - A measure from 0.0 to 1.0 describing the musical positiveness conveyed by a track. Tracks with high valence sound more positive (e.g. happy, cheerful, euphoric), while tracks with low valence sound more negative (e.g. sad, depressed, angry).
16. Tempo - 	The overall estimated tempo of a track in beats per minute (BPM). In musical terminology, tempo is the speed or pace of a given piece and derives directly from the average beat duration.
17. Time Signature - An estimated overall time signature of a track. The time signature (meter) is a notational convention to specify how many beats are in each bar (or measure).
18. Duration (ms) - The duration of the track in milliseconds.


## How many tracks on average, does an artist release prior to becoming popular?
  In this question, we analyze whether there is a relationship between an artist's Spotify *follower*  count and the number of *tracks* they produced. Our preliminary analysis revealed that mislabeled "artist" accounts, particularly music complication and bot accounts, were significant skewing the data distribution. These "artist" accounts had significantly higher track counts than the average artist. We address this issue by removing these data points, since they do not reflect the type of artist we study in this work.

  The scatter plot between follower count and track count (with outliers removed) was too noisy to discern a meaningful relationship between the two variables. To address this issue, we created a subset of the outlier-free data. Specifically, data points within &#177; 5 of the 90th percentile popularity threshold were selected and visualized to see if a relationship exists around the popularity boundary. The plots are shown below.  

![Plot #1](https://github.com/Coldestadam/SpotifyBSDS200/blob/master/plot/follower_track_relationship(most500).png?raw=true)
![Plot #2](https://github.com/Coldestadam/SpotifyBSDS200/blob/master/plot/follower_track_relationship(most200).png?raw=true)

 The first plot shows the relationship between the two variables with outliers removed and artists between the 85th and 95th percentiles. The second plot filters the data further, showing artists that have at most 200 tracks. Both plots show no clear relationship between follower count and track count. However, an interesting pattern can be seen in the left corners of both plots: many artists with 1 to 25 tracks are popular, which was not what we expected.

 To understand the low track count but high follower count result, we separated data points into 9 bins each of size 25. Then, each artist was assigned to a bin based on the number of tracks they had produced. Finally, we calculate the average follower count per bin, and the resulting table is shown below.

Total Tracks | Average Followers
------------ | -----------------
200+ | 1,583,177
176-200 | 1,987,108
151-175 | 897,072
126-150 | 1,354,869
101-125 | 1,283,653
76-100 | 855,795
51-75 | 638,394
26-50 | 550,359
1-25 | 343,192

From the table above, we see that follower count generally increases when track counts are higher. This is interesting since previous results suggested otherwise.

While the table above suggests that producing more music may increase an artist's popular, it doesn't explain why some artists become popular after producing only a few tracks. To this end, we selected the five most popular artists with only 1 track in their repertoire and present the results below.       


Artist Name | Follower Count | Instagram
----------- | -------------- | ---------
Mc Marechal | 145,567 | [https://instagram.com/mcmarechal](https://instagram.com/mcmarechal)
Zeph | 11,359 | [https://instagram.com/zephanijong](https://instagram.com/zephanijong)
Doubleu | 2034 | [https://www.instagram.com/double_the_u](https://www.instagram.com/double_the_u)
Ava Beathard | 1247 | [https://instagram.com/avabeathard](https://instagram.com/avabeathard)
Iasmin | 1126 | [https://instagram.com/iasmin.cantora](https://instagram.com/iasmin.cantora)

We find that the majority of popular artists with few tracks consist of "influencers" from other entertainment domains (i.e., YouTube, TikTok, Instagram, etc.). However, Mc Marechal  happens to be an outlier in this regard since he is a well-known Brazilian Rapper. Nonetheless, the others seem to be influencers on Instagram and other online platforms like YouTube. To be clear, this list does not provide a completely accurate representation of the data, since it was created by heavily filtering dataset. However, the observation that influencers with large fanbase tend to be successful despite releasing fewer tracks on average makes sense.


## How much does an artist's Twitter presence impact their popularity?

  We answered this question using a variety of methods--each one being generally more complex than the prior method. Due to time constraints, we elected to do the simplest approach: calculating correlation scores between variables. This score can indicate the strength and direction of a linear relationship between the target variable (Spotify follower count) and an independent variable (e.g. a Twitter metric). One limitation of this approach is that the correlation score will not be very helpful if the relationship between variables is non-linear. Below, we’ve generated a correlation matrix to see which Twitter metrics have the strongest correlation with Spotify follower counts.

![Plot #3](https://github.com/Coldestadam/SpotifyBSDS200/blob/master/plot/follower_count_twitter_metrics_corr.png?raw=true)

  The above plot suggests that the correlation between Twitter followers and Spotify Follower count is the strongest positive relationship (0.64). Aside from that the Twitter variable with the second-highest correlation is verified at only 0.18, suggesting that having a verified account on Twitter slightly improves follower count.

  To get a better understanding of the linearity of the artists twitter data, we also plotted a scatter plot matrix:

![Plot #4](https://github.com/Coldestadam/SpotifyBSDS200/blob/master/plot/follower_count_twitter_metrics_scatterMatrix.png?raw=true)

  The scatter plot matrix suggests the relationship between follower counts and twitter metrics is non-linear for every variable except Twitter followers.

  The correlation matrix suggests there's no strong linear relationships between the Twitter metrics ('followers', 'following', 'likes', 'tweets', 'verified') and Spotify follower count. In addition, the scatter plot matrix confirmed our hypothesis that the relationship between twitter metrics and artist follower counts was non-linear. Despite this, we constructed a logistic regression model. However, we recognize that the usefulness of this model is questionable, we plan to explore this in future work. The next few paragraphs describe the logistic regression model.

  Using a logistic regression (Logit) model where our outcome is binary (0 or 1), where 0 means the artist is not popular and 1 means they are popular, we can attempt to predict artist popularity based on their corresponding Twitter metrics. The equation for this model is very similar to the linear regression equation except we replace our target variable, Spotify follower count, with a variable to indicate whether an artist is popular or not. The equation is shown below:

> *popular = B0 + B1(‘tweets’) + B2(‘likes’) + B3(‘following’)+ B4(‘followers’) + B5(‘media’) + B6(‘verified’) + B7(‘spotify_follower_ct’) + e*

  Using the SciKit Learn Python library, we built a logistic regression model to classify artists as either 'popular' or 'not popular' based on their twitter metrics. The data processing and results are described in the following paragraphs.

  We first pulled our data from our SQL database, and calculated the 90th percentile of artist popularity to create our Y target column, ‘popular’. We proceeded to label all of the artists in our data frame with either 1 or 0, depending on whether the artist was above the popularity threshold or not. Since the ‘verified’ Twitter variable is categorical, we converted it to a *one-hot-encoded* variable to include it in our model.

  We split our data into 80% for training, and 20% in for testing. We achieved 99% accuracy with very low false-positive and false-negative rates. The confusion matrix can be seen below.

![Plot #5](https://github.com/Coldestadam/SpotifyBSDS200/blob/master/plot/logit_confusion_matrix.png?raw=true)


  Our analysis supports that the artist popularity data is heavily skewed with 34,111 ‘not popular’ artists, and only 3,790 ‘popular’ artists, which likely is similar to the real-world distribution of popular versus non-popular artists. Consequently, we don’t expect the model to generalize well to out of distribution data, despite its high accuracy. We leave exploring more data and different models to future work.

## What are the correlations between audio characteristics (e.g., acousticness, valence, key) and artist popularity?

  We looked at the artist-level audio characteristics data and determine the correlation between audio characteristics and artist popularity. There are approximately 128 thousand unique artists. Each artist has produced roughly 8 tracks. Each track has a set of audio features. These features estimate a track’s overall: valence, danceability, energy, loudness, speechiness, acousticness, instrumentalness, liveness, valence, tempo, time signature, and duration (ms). We calculated artist-level features by taking the average feature values of each track grouped by artist_id.

  We calculated the correlation matrix between Spotify artist follower counts (e.g., artist popularities) and artist-level audio features and plotted the correlation scores between each variable using a heatmap. This information can help us better understand the influence of audio features on artist popularity.

![Plot #6](https://github.com/Coldestadam/SpotifyBSDS200/blob/master/plot/follower_count_audio_characteristics_corr.png?raw=true)

  The correlation matrix between Spotify follower count and average audio characteristics of artists suggests that loudness, valence, and liveness may be the least negatively associated with Spotify follower count. Additionally, if we look carefully, instrumentalness appears to negatively correlate with follower count. That is to say, artists with more instrumental music may have fewer Spotify followers. Lastly, audio characteristics are actually associated with tracks, we took the average of audio characteristics of all tracks by an artist to calculate an artist’s audio characteristics. By reducing our data in this way we may miss out on important information, which could explain why no audio feature displays a positive relationship with Spotify follower count. Ideally, to overcome this, we would use a different aggregation method for artist-level audio features such as selecting the features from the top songs (i.e. most popular) for each artist and then averaging them. However, our dataset does not include track popularity so this approach is not possible. In future work, we may try to collect and incorporate track popularity into our analysis.

  ![Plot #7](./plot/follower_count_audio_characteristics_scatterMatrix.png)

   The scatter plot matrix between the artist follower counts variable (left most column) and a few of the average audio metrics. We can see that both danceability and energy display a relatively linear relationship with follower counts. In contrast, variables such as loudness appears to follow the [power-law distribution](https://en.wikipedia.org/wiki/Power_law), while tempo resembles the Normal distribution with respect to and follower count.

# Conclusions

  Our initial analysis of Spotify and Twitter data for artists indicates that the distribution between artist popularity and track counts is highly skewed, and the relationship is non-linear. To better understand the relationship, we clustered popular artists into bins based on track counts. We set the bin size to 25 and chose to focus on the most popular artists in the first ten bins (i.e., popular artists with 200 or fewer total tracks). We looked at a handful of artists with one track and reviewed their social media and spotify artist profiles. We found that many of these popular artists with few tracks were “influencers” from domains outside of music. For example, popular YouTubers and Instagram “influencers” were common among the selected group of popular artists. This confirms our hypothesis that popularity in one entertainment domain may lead to popularity in another domain.

   In addition, we find that the Twitter followers variable seems to be the only Twitter metric that has a linear relationship with the follower count variable. Furthermore, the logistic model we built achieved extraordinary metrics that included 99% accuracy, and less than 1% false-positive and false-negative rates. Unfortunately, we don’t expect this model to generalize well due to the fact that our training and testing data was heavily skewed. We leave exploring different models, and scraping more data to further work.

  For the the correlations between the average artist audio characteristics (e.g., acousticness, valence, key) variables and artist popularity, we find that loudness, valence, and liveness are the least negatively associated with Spotify follower count, while instrumentalness appears to be negatively correlated with follower count. This suggests that artists that have more instrumental-based music may be less popular. However, the scatter plot matrix for these variables suggest that many of them do not follow a linear distribution, and instead, they follow a power-law distribution.
