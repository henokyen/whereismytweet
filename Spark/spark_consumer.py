import sys
import redis
import conf
import cfg
import time 
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

try:
    import json
except ImportError:
    import simplejson as json



# some parametres 
tweetId = 0
tlast = 0
tsince = 0
userlist = conf.userlist
int_userlist = [int(x) for x in userlist]
DATE_FORMAT = "%a %b %d %H:%M:%S +0000 %Y"


def getTweetTime(data):
	stime = time.strptime(data['created_at'],DATE_FORMAT)
	epochtime = time.mktime(stime)* 1000.0
	return epochtime

#process each tweet in each rdd and return (tweetID, retweet_info) tube 
def getTweet(tweet):
	global broadtweetID
	try:
           userdict = dict({'id':tweet['user']['id'],
                     'screen_name':tweet['user']['screen_name'],
                     'followers_count':tweet['user']['followers_count'],
                     'color':0,
                     'rt_time':getTweetTime(tweet)})
           return (int(broadtweetID.value),json.dumps(userdict,ensure_ascii=True))
        except KeyError:
		 return()     
     
def processRDDs(rdd):
  global broadtweetID
  # reads the rdd as a json object and takes only the value leaving the key
  parsed = rdd.map(lambda v:json.loads(v[1]))
  print "Filtering for %s" %broadtweetID.value
  tweet = parsed.filter(lambda tw: 'retweeted_status' in tw)
  rt_sorted = tweet.sortBy(lambda x: getTweetTime(x))    
  retweet = rt_sorted.filter(lambda t : t['retweeted_status']['id'] == int(broadtweetID.value))\
            .map(lambda x : getTweet(x)).groupByKey()\
            .map(lambda x: (x[0], x[1])).collect()  
  
  # if we have retweets of the original tweet 
  
  if len(retweet) != 0:
    print "Writting retweet to the redis database..."
    dicts = list(retweet[0][1])    
    for i in range (0,len(dicts)):
        print dicts[i]
        cfg.red.lpush(retweet[0][0],dicts[i]) 
       
if __name__=="__main__":
 sc = SparkContext(appName="PythonStreamingKafkaPrintTwitter")
 ssc = StreamingContext(sc, 2)
 
 kafkaBrokers = {"metadata.broker.list": "52.33.140.25:9092, 52.40.222.134:9092,52.40.28.231:9092,50.112.180.207:9092"}
 topic = 'Donald_Retweet'
 tweets = KafkaUtils.createDirectStream(ssc, [topic],kafkaBrokers)
	
 print "Reading the tweetID from Redis ...."
 # Check  if Redis for start signal
 while True:
   if cfg.red.llen('start') == 0:
     time.sleep(30)
   else: 
       tweetID = cfg.red.lindex('start',0)
       break;
 print "Each worker is looking for a retweet having a tweetID %s" %tweetID
 print "Broadcastting %s to each worker" %tweetID 
 broadtweetID = sc.broadcast(tweetID)
 tweets.foreachRDD(processRDDs)

 ssc.start()
 ssc.awaitTermination() 
