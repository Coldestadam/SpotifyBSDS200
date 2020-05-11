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

### Variables
If you become confused during the report regarding our variables, please return here!

1. Follower Count - The number of Spotify followers for each artist
2. Track Count - The number of tracks that each artists has on Spotify
3. Twitter Followers - The number of Twitter followers per artist
4. Twitter Following - The number of twitter accounts that the artist follows
5. Twitter Likes - The number of Twitter likes that an artist has
6. Twitter Tweets - The numer of tweets (Twitter Posts) that an artist has
7. Twitter Verified - Details whether the artist is verified on Twitter
8. **BRIAN PUT YOURS HERE**

## How many tracks on average, does an artist release prior to becoming popular?
  We used SQL to gather data to solve this problem, and the variables we are the total number of *followers* and *tracks* that each artist has made. Once we collected the data, we looked at the distributions of my variables and saw what changes can be made to the SQL query, such as the removal of outliers. After, we began looking at the relationship between the number of followers and track count to see if there is some linear or exponential relationship. We viewied this relationship through our interpretation of the popularity threshold, which is the 90th percentile of the number of followers of all our Spotify artists. In other words, we got the number of followers that 90% of Spotify artists do not have, but 10% of artists have followers above that number. That 10% of all artists are considered "popular".
  
  With this, we can created a subset of data around our popularity threshold to see what is the average number of tracks that artists have when they are close to being popular. To answer the question, we plotted the relationship between the number of tracks and followers for the artist in our subset. Further, we averaged the amount of followers for separated categories that detailed a range for the number of tracks, and we found the reason why there are popular artists who have one track.

![Plot #1](https://github.com/Coldestadam/SpotifyBSDS200/blob/master/plot/follower_track_relationship(most500).png?raw=true)
![Plot #2](https://github.com/Coldestadam/SpotifyBSDS200/blob/master/plot/follower_track_relationship(most200).png?raw=true)

These plots show that there is really no linear or exponential relationship between follower count and the total number of tracks an artist has. The left plot shows the relationship of the data given through the SQL query and the right plot shows data only of artists that have at most 200 tracks. It seems that many artists with 1-25 tracks are considered popular, which was not what we expected. We expected there to be somewhat of a gradual increase of followers when the number of tracks increases.

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
