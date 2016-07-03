import os
import json
import time
import sys
import fileinput
import redis
import cfg
from kafka import KafkaProducer
from datetime import datetime
import fileinput
try:
    import json
except ImportError:
    import simplejson as json

"""
Given a tweet, returns basic information about that tweet as a python dictionary. 
Size and color are additional attributes used to render tweets as nodes in graph.
"""	
class Tweet_Producer(object):

    def extractDict(self,status):
      userdict = dict({'id':status['user']['id'],
                     'screen_name':status['user']['screen_name'],
                     'followers_count':status['user']['followers_count'],
                     'size':10,
                      'color':7})                   

      return json.dumps(userdict, ensure_ascii=True) 
    
    """Initialize Producer with address of the kafka broker ip address."""
    def __init__(self, addr):
       
        self.producer = KafkaProducer(bootstrap_servers=addr,value_serializer=lambda v: json.dumps(v).encode('ascii'))
    
    """
    from stream of tweets, look for a tweet by a specific user. If one is found, produce a kafka topic of all tweets. 
    These tweets will be filtered by consumera, which are a spark streaming processes   
    """    
    def streamretweets(self):
		global counter 	
		# a stream of tweets are read from a file to simulate influx of tweets from Twitter 
		tweets_filename = 'soc_retweet.txt'       
		tweets_file = open(tweets_filename, "r")     
		   
		for line in fileinput.input(tweets_filename):
		   tweet = json.loads(line) 
		   if ('retweeted_status' not in tweet) and (tweet['user']['id'] in int_userlist): 
               print (" Caught a tweet with %s" % tweet['id']) 
		       tweetID = tweet['id']
	           if cfg.red.llen('start') == 0: 
                  cfg.red.lpush('start',tweetID) # to indicate that a user has tweeted and building the graph can start 
                  cfg.red.lpush("Orig",self.extractDict(tweet))
		       counter += 1
		   #when it is know that a user has tweeted, a kafka topic is produced 
		   if (counter >= 1):			
			self.producer.send('Donald_Retweet',tweet)
		   #stop the stream when enough retweets are collected 	
		   if (counter == cfg.limit):
				print("Collected enough tweets, shutting down stream...")
				red.lpush(tweetID, "Stop") # to indicate that a tweet has received enough retweets and building the graph can stop
				return False
            
		
#define and initialize some variables 
counter = 0
tweetId = 0
userlist = cfg.userlist
int_userlist = [int(x) for x in userlist]
DATE_FORMAT = "%a %b %d %H:%M:%S +0000 %Y"
if __name__ == "__main__":    
    args = sys.argv
    kafka_url = "{}:9092".format(str(args[1])) 
    print(kafka_url)
    partition_key = str(args[2])
    prod = Tweet_Producer(str(args[1]))    
    prod.streamretweets()

