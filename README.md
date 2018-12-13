# Energing topic detector and Sentiment Analysis on the emerging topic tweets

This project experiments with Twitter streaming and various Spark streaming APIs(DStream abstraction)

The goal is to determine the emerging topic in Twitter. For example, if the window duration is set to 1 hour, this application determines the most emerged topic in that hour.
The hashtag with the greatest increase in count compared to the last one hour is defined as the emerging hashtag.

Note: This is not just the hashtag with maximum count in a window. It is the hashtag with maximum count difference when compared to the previous window.
So, this application involves stateful streaming.

Once the emerging hashtag is determined, all the tweets corresponding to that hashtag are filtered and sentiment analysis is done on those tweets to determine the public opinion.
