import os
import json
import time
import random
import sys
import fileinput
import redis
import cfg
from kafka import KafkaProducer
import random
import sys
import six
from datetime import datetime
import fileinput
try:
    import json
except ImportError:
    import simplejson as json

#define and initialize some variables 
tweetID = 0 
counter = 0
userlist = cfg.userlist
int_userlist = [int(x) for x in userlist]

class TweetProducer(object):
	"""
	Given a tweet, returns basic information from the tweet in python dictionary
	"""	
    def extractDict(self,status):
      userdict = dict({'id':status['user']['id'],
                     'screen_name':status['user']['screen_name'],
                     'followers_count':status['user']['followers_count'],
                      'color':7})                    

      return json.dumps(userdict, ensure_ascii=True) 
    def __init__(self, addr):
        """Initialize Producer with address of the kafka broker ip address."""
        self.producer = KafkaProducer(bootstrap_servers=addr,value_serializer=lambda v: json.dumps(v).encode('ascii'))
    
    """
    from stream of tweets, look for a tweet by a specific user. If one is found, then produce a kafka topic of all tweets. 
    The tweets will be processed (filtered) from the counsumer side, which is a spark stream   
    """    
    def StreamRetweets(self):
		global counter , int_userlist, tweetID
		# a stream of tweets are simulated from a file 
		tweets_filename = 'soc_retweet.txt'       
		tweets_file = open(tweets_filename, "r")        
		for line in fileinput.input(tweets_filename):
		   tweet = json.loads(line) 
		   if ('retweeted_status' not in tweet) and (tweet['user']['id'] in int_userlist): 
               print (" Caught a tweet with %s" % tweet['id']) 
		       tweetID = tweet['id']
	           if cfg.red.llen('start') == 0: 
                  cfg.red.lpush('start',tweetID)
                  cfg.red.lpush("Orig",self.extractDict(tweet))
		       counter += 1		   
		   if (counter >= 1):			
			self.producer.send('Donald_Retweet',tweet)




if __name__ == "__main__":   
    args = sys.argv
    kafka_url = "{}:9092".format(str(args[1]))    
    partition_key = str(args[2])
    producer = TweetProducer(str(args[1]))    
    producer.StreamRetweets()

