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

We were able to gather data of Spotify artists and we became interested of what makes an artist "popular". We determined artists' popularity based on the number of Spotify followers they have and we contained other variables as well. As a glimpse of the other variables we used, we have the number of tracks artists have and the audio characteristics of their music. In this report, we dive further down to discover what variables can lead to an increase of followers that an artist can have, or rather their popularity.

### Variables Used
If you become confused during the report regarding our variables, please return here!

1. Follower Count - The number of Spotify followers for each artist
2. Track Count - The number of tracks that each artists has on Spotify
3. Twitter Followers - The number of Twitter followers per artist
4. Twitter Following - The number of twitter accounts that the artist follows
5. Twitter Likes - The number of Twitter likes that an artist has
6. Twitter Tweets - The numer of tweets (Twitter Posts) that an artist has
7. Twitter Verified - Details whether the artist is verified on Twitter
8. **Kai PUT YOURS HERE**

## How many tracks on average, does an artist release prior to becoming popular?
  We used SQL to gather data to solve this problem, and the variables we are the total number of *followers* and *tracks* that each artist has made. Once we collected the data, we looked at the distributions of the variables and saw what changes can be made to the SQL query, such as the removal of outliers. After, we began looking at the relationship between the number of followers and track count to see if there is some linear or exponential relationship. We viewied this relationship through our interpretation of the popularity threshold, which is the 90th percentile of the number of followers of all our Spotify artists. In other words, we got the number of followers that 90% of Spotify artists do not have, but 10% of artists have followers above that number. That 10% of all artists are considered "popular".
  
  With this, we can created a subset of data around our popularity threshold to see what is the average number of tracks that artists have when they are close to being popular. To answer the question, we plotted the relationship between the number of tracks and followers for the artist in our subset. Further, we averaged the amount of followers for separated categories that detailed a range for the number of tracks, and we found the reason why there are popular artists who have one track.

![Plot #1](https://github.com/Coldestadam/SpotifyBSDS200/blob/master/plot/follower_track_relationship(most500).png?raw=true)
![Plot #2](https://github.com/Coldestadam/SpotifyBSDS200/blob/master/plot/follower_track_relationship(most200).png?raw=true)

These plots show that there is really no linear or exponential relationship between follower count and the total number of tracks an artist has. The first plot shows the relationship of the data given through the SQL query and the second plot shows data only of artists that have at most 200 tracks. It seems that many artists with 1-25 tracks are considered popular, which was not what we expected. We expected there to be somewhat of a gradual increase of followers when the number of tracks increases.


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

The Table above shows the average follower counts of popular artists per category of tracks.


Artist Name | Follower Count | Instagram
----------- | -------------- | ---------
Mc Marechal | 145,567 | [https://instagram.com/mcmarechal](https://instagram.com/mcmarechal)
Zeph | 11,359 | [https://instagram.com/zephanijong](https://instagram.com/zephanijong)
Doubleu | 2034 | [https://www.instagram.com/double_the_u](https://www.instagram.com/double_the_u)
Ava Beathard | 1247 | [https://instagram.com/avabeathard](https://instagram.com/avabeathard)
Iasmin | 1126 | [https://instagram.com/iasmin.cantora](https://instagram.com/iasmin.cantora)

This is a list of the top 5 artists who only have one track. Mc Marechal is considered popular and he happens to be a Brazilian Rapper, however the others seem to be influencers on Instagram and other online platforms like YouTube. To be clear, this list does not represent the data fairly, this list was created with a heavily filtered dataset through many inner joins in the query. However, the idea that influencers who have a large fanbase who create one track that considers them popular on Spotify seems to be true within the normal dataset.


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

**PUT YOUR PLOT HERE BRIAN**

  Unfortunately, we have come to the realization that our data is heavily skewed with 34,111 ‘not popular’ artists, and only 3,790 ‘popular’ artists, which likely is similar to the real-world distribution of popular versus non-popular artists. Despite the fact that our model has such high accuracy, we don’t expect it to generalize well due to the data distribution. We leave exploring different models, and scraping more data to further work.

## What are the correlations between audio characteristics (e.g., acousticness, valence, key) and artist popularity?

  We looked at the artist-level audio characteristics data and determine the correlation between audio characteristics and artist popularity. There are approximately 128 thousand unique artists. Each artist has produced roughly 8 tracks. Each track has a set of audio features. These features estimate a track’s overall: valence, danceability, energy, loudness, speechiness, acousticness, instrumentalness, liveness, valence, tempo, time signature, and duration (ms). We calculated artist-level features by taking the average feature values of each track grouped by artist_id.

  We calculated the correlation matrix between Spotify artist follower counts (e.g., artist popularities) and artist-level audio features and plotted the correlation scores between each variable using a heatmap. This information can help us better understand the influence of audio features on artist popularity.
  
![Plot #6](https://github.com/Coldestadam/SpotifyBSDS200/blob/master/plot/follower_count_audio_characteristics_corr.png?raw=true)

  The correlation matrix between Spotify follower count and average audio characteristics of artists suggests that loudness, valence, and liveness may be the least negatively associated with Spotify follower count. Additionally, if we look carefully, instrumentalness appears to negatively correlate with follower count. That is to say, artists with more instrumental music may have fewer Spotify followers. Lastly, audio characteristics are actually associated with tracks, we took the average of audio characteristics of all tracks by an artist to calculate an artist’s audio characteristics. By reducing our data in this way we may miss out on important information, which could explain why no audio feature displays a positive relationship with Spotify follower count. Ideally, to overcome this, we would use a different aggregation method for artist-level audio features such as selecting the features from the top songs (i.e. most popular) for each artist and then averaging them. However, our dataset does not include track popularity so this approach is not possible. In future work, we may try to collect and incorporate track popularity into our analysis.
  
  ![Plot #7](https://github.com/Coldestadam/SpotifyBSDS200/blob/master/plot/follower_count_audio_characteristics_scatterMatrix.png?raw=true)
  
   The scatter plot matrix between the artist follower counts variable (left most column) and average audio metrics for each artist. While it is nearly impossible to read the column names **(we plan to fix this in the final version)**, we can see that variables such as danceability and energy may share a linear relationship with follower counts, while variables such as loudness appear to follow the [power-law distribution](https://en.wikipedia.org/wiki/Power_law).
  
# Conclusions

  Our initial analysis of Spotify and Twitter data for artists indicates that the distribution between artist popularity and track counts is highly skewed, and the relationship is non-linear. To better understand the relationship, we clustered popular artists into bins based on track counts. We set the bin size to 25 and chose to focus on the most popular artists in the first ten bins (i.e., popular artists with 200 or fewer total tracks). We looked at a handful of artists with one track and reviewed their social media and spotify artist profiles. We found that many of these popular artists with few tracks were “influencers” from domains outside of music. For example, popular YouTubers and Instagram “influencers” were common among the selected group of popular artists. This confirms our hypothesis that popularity in one entertainment domain may lead to popularity in another domain.
  
   In addition, we find that the Twitter followers variable seems to be the only Twitter metric that has a linear relationship with the follower count variable. Furthermore, the logistic model we built achieved extraordinary metrics that included 99% accuracy, and less than 1% false-positive and false-negative rates. Unfortunately, we don’t expect this model to generalize well due to the fact that our training and testing data was heavily skewed. We leave exploring different models, and scraping more data to further work. 
   
  For the the correlations between the average artist audio characteristics (e.g., acousticness, valence, key) variables and artist popularity, we find that loudness, valence, and liveness are the least negatively associated with Spotify follower count, while instrumentalness appears to be negatively correlated with follower count. This suggests that artists that have more instrumental-based music may be less popular. However, the scatter plot matrix for these variables suggest that many of them do not follow a linear distribution, and instead, they follow a power-law distribution.
