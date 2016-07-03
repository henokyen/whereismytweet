import redis
import cfg
import time 
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
try:
    import json
except ImportError:
    import simplejson as json

#For each tweet return some attributes as a python dictinary 
def getTweet(tweet):	
	try:
           userdict = dict({'id':tweet['user']['id'],
                     'screen_name':tweet['user']['screen_name'],
                     'followers_count':tweet['user']['followers_count'],
                     'color':10,
                     'size':5,
                     'rt_time':cfg.getTweetTime(tweet)})
           return (json.dumps(userdict,ensure_ascii=True))
        except KeyError:
		 return()     

#tweets are filtered
def ProcessRDDs(rdd):
  
  global broadtweetID
  
  #reads the rdd as a json object and takes only the value, leaving the key
  parsed = rdd.map(lambda v:json.loads(v[1]))  
  
  #filter out retweets only 
  tweet = parsed.filter(lambda tw: 'retweeted_status' in tw)
  
  #then, select retweets that are retweets of the original tweet, and sort them based on their creation time 
  retweet = tweet.filter(lambda t : t['retweeted_status']['id'] == int(broadtweetID.value))\
            .sortBy(lambda re_time: cfg.getTweetTime(re_time))\
            .map(lambda x : getTweet(x)).collect()
            
  
  # store the sorted retweets in redis cache with the id of the original tweet as a key   
  if len(retweet) != 0:    
    for i in range (0,len(retweet)):     
        cfg.red.lpush(int(broadtweetID.value),retweet[i]) 

   
if __name__=="__main__":
 sc = SparkContext(appName="PythonStreamingKafkaPrintTwitter")
 ssc = StreamingContext(sc, 2)
 kafkaBrokers = {"metadata.broker.list": "52.33.140.25:9092, 52.40.222.134:9092,52.40.28.231:9092,50.112.180.207:9092"}
 topic = 'Donald_Retweet'
 tweets = KafkaUtils.createDirectStream(ssc, [topic],kafkaBrokers)
	
 print "Reading the tweetID from Redis ...."
 # Continuously check if retweets arrive 
 while True:
   if cfg.red.llen('start') == 0:
     time.sleep(30)
   else: 
       tweetID = cfg.red.lindex('start',0)
       break;
 print "Each worker is looking for retweets of a tweet with tweetID %s" %tweetID
 print "Broadcastting %s to each worker" %tweetID 
 #each spark streaming process is going to select re-tweets of a tweet with this tweetID, 
 #so this Id is broadcated to each worker node
 broadtweetID = sc.broadcast(tweetID) 
 
 
 tweets.foreachRDD(ProcessRDDs)
 ssc.start()
 ssc.awaitTermination() 
