# whereismytweet
A real-time graph construction from retweets of a tweet
 
#Introduction 
The idea is to graph re-tweets of a tweet for the purpose of visualizing how that tweets has spread out across followers and followers-of-followers.

#Technologies 
In this project, I used kafka to ingest tweets, which are  streamed from Twitter through a twitter public streaming API. Spark streaming is also employed to process these incoming tweets. With this process, re-tweets of a specific tweets are collected. Because re-tweets can arrive out of order, the filtered re-tweets are sorted based on their creation time. The the re-tweet graph is built from the sorted re-tweets. A Redis data store is used to cache this graph for later retrieval. The flask web development tool is used to display the graph.
#The outline of the data pipeline
[alt tag](https://github.com/henokyen/whereismytweet/InsightDataengineering_Henok_architecture.pdf)
