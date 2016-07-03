# whereismytweet
A real-time graph construction from retweets of a tweet
 
#Introduction 
The idea is to graph re-tweets of a tweet for the purpose of visualizing how that tweets has spread out across followers and followers-of-followers.

#Data Pipeline 
The process starts by listening for a tweet by a specific user. After one is found, the upcoming tweets are ingested in to the pipelien through kafka. The tweets are streamed from Twitter through a twitter public streaming API. Spark streaming is also employed to filter and sort these incoming tweets. Because re-tweets can arrive out of order, they are sorted based on their creation time. The the re-tweet graph is built from these sorted re-tweets. A Redis data store is used to cache this graph for later retrieval. The flask web development tool is used to display the graph.

![alt tag](https://raw.githubusercontent.com/henokyen/whereismytweet/master/InsightDataengineering_Henok_architecture.png)

