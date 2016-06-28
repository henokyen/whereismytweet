# whereismytweet
A real time graph construction for retweets
 
#Introduction 
The idea is to graph re-tweets to visualize how a tweets spreads out across followers and followers-of-followers.

#Technologies 
In this project, I used kafka to ingest tweets, which are  streamed from Twitter through a twitter streaming API. Spark streaming is also employed to process these incoming tweets. Through this process re-tweets of a specific tweets are collected. Because re-tweets can arrive out of order, the filtered re-tweets are sorted based on their creation time. A graph is made out of the sorted re-tweets. A Redis data store is used to cache this graph for later retrieval.  The flask web development tool is used to display the graph 
