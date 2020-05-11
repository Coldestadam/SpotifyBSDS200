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

We were able to gather data of Spotify artists and we became interested of what makes an artist "popular". We determined artists' popularity based on the number of Spotify followers they have and we contained other variables as well. As a glimpse of the other variables we used, we have the number of tracks artists have and the audio characteristics of their music. In this report, we dive further down to discover what variables can lead to an increase of followers artists have, or rather their popularity.

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
  The way we plan to solve this problem is by first using SQL to gather data to solve this problem. The variables we need to gather are the total number of *followers* and *tracks* that each artist has made. Once we have the data, we will look at the distributions of my variables and see what changes can be made, such as the removal of outliers. Then we will go further by looking at the relationship between the number of followers and track count to see if there is some linear or exponential relationship. We will view this relationship through our interpretation of the popularity threshold, which is the 90th percentile of the number of followers of all our Spotify artists. With this, we can create a subset of data around our popularity threshold to see what is the average number of tracks that artists have when they are close to being popular.
  For future work, we aim to better understand the relationship between track counts and artist popularity for artists with few tracks. To this end, we will average the amount of followers for categories of the number of tracks, and the reason why there are popular artists who have one track.
