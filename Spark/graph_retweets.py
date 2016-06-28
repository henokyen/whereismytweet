# Builds the retweet graph from the retweets stored in Redis

import cfg
import sys
import tweepy
import time
import json
import redis
import os
from priority_dict import priority_dict
from retweet import Retweet

REDIS_FRIENDS_KEY = "friends:%s" 

# Since retweets can arrive out of order, we need to sort them based on their creation time 
# I used a min-heap python dictioary to sort retweets. In this dictionary tweets are the keys and creation times are values
dict_retweet_set = priority_dict()

# To hold the resulting graph: 2 lists
links = []
nodes = []

# decods a json string as a Retweet class object 
def json_decoder(obj):
    return (Retweet(obj))

def getTweetTime(data):
        stime = time.strptime(data['created_at'],cfg.DATE_FORMAT)
        epochtime = time.mktime(stime)* 1000.0
        return epochtime
# given a tweet returns basic information about that tweet as a dictionary 
def extractDict(status):-
    userdict = dict({'id':status['user']['id'],
                     'screen_name':status['user']['screen_name'],
                     'followers_count':status['user']['followers_count'],
                     'rt_time':cfg.dt_to_epoch(status['created_at'])})    
    return userdict 

def addToGraph (parent, child):
    global links
    if (child):
          nodes.append(child)
	  if (parent):
	      links.append({'source':getNodeIndex(parent), 
                          'target':getNodeIndex(child)})
def getNodeIndex (user):
    global nodes

    for i in range(len(nodes)): 
        
        if (user.id == nodes[i].id):
            return i
    return -1

# form the neo4j database, fetch users who are followed by this specific user
def fecthFriends(user):
  userid = "User_"+str(user.id)  
  rel = "MATCH (a:User)-[:Follows]->(b:User {name: {S}}) RETURN a.name as name"
  follwed_list = list(cfg.graph.cypher.execute(rel,{"S":userid})) 
  for followed in follwed_list:
	cfg.red.hset("friends:%s" % user.id, followed.name.strip("User_"), "")     

def isFriend(parent,child):
    # if child hasn't been crawled yet, get all the people this child could have retweeted
    # Note getting this information from Twitter is very time consuming    
    key = REDIS_FRIENDS_KEY % child.id
    if (not cfg.red.exists(key)):
          fecthFriends(child) 
    return cfg.red.hexists(key, parent.id) #returns 1 if any existing node is being followed by this child, 0 otherwise

def reverseSearch(user,source):
    global nodes, connected

    # discard if duplicate, is that retweet is already part of the graph 
    if user in nodes:
        return 

    # assume node is isolated until parent is found
    parent = None

    # connect user by iterating through already-connected nodes, i.e., find from which existing user this current user might have retweeted
    for existing in connected:
        if isFriend(existing,user):
            parent = existing
            break

    if parent is not None: 
        print ("    New edge: %s <=== %s" % (parent.screen_name, user.screen_name))
        addToGraph(parent, user)
    else:
        print ("    User %s is isolated" % user.screen_name)
        addToGraph(None, user)

    # retweeter has been connected
    connected.append(user)

-
# Main Program

# optional: supply tweetId to pick up from redis, if it is still in memmory otherwise read it from disk
if len(sys.argv) == 2:
    tweetId = sys.argv[1]

#if tweetId is supplied, do not wait on Redis signal. Mostly, it is hard to know the id of a certain tweet
if len(sys.argv) == 2:
    tweetId = sys.argv[1]
else:
    #continuously check Redis for start signal, meaning if a user tweets
    while True:
        if cfg.red.llen('start') == 0:
            time.sleep(30)
        else:
            tweetId = cfg.red.lindex('start',0)
            break


retweetiD = tweetId + "i"
if cfg.red.llen(retweetiD)!= 0:
 print "Loading a retweet graph from Redis keyed at: ",retweetiD
 data = json.loads(cfg.red.lindex(retweetiD,0))
 nodes = [json.loads(json.dumps(n), object_hook=json_decoder) for n in data["nodes"]]
 links = data["links"]
 # set the root
 root = nodes[0]
 # populate connected
 connected = nodes

else:        
    #otherwise, retrive the orignal tweet 
    root = json.loads(cfg.red.lindex("Orig",0))
    rt_time = root ['rt_time']
    root = Retweet(root)
    dict_retweet_set[root] =rt_time
    connected.append(root)
    addToGraph(None, root)


# iterate over CURRENT list of unconnected retweets from Redis
quit = 0
while (quit == 0):
    # check for "Stop" signal, i.e., if enough retweet has been collected, then lpop it
    if (cfg.red.lindex(tweetId,0) == "Stop"): 
        cfg.red.lpop(tweetId)
        quit = 1

    # if the list is empty (i.e., if there is no retweets) sleep and wait for retweets to arrive 
    # llen is the number of people who retweets the tweet with tweetId
    
    llen = cfg.red.llen(tweetId) 
    if llen == 0:
        print("Retweet queue is empty, sleeping for 30 seconds.")
        time.sleep(30)
        continue
    # otherwise, process the retweets and form the graph off them 
    print("Forming a graph with %s retweets retrieved from Redis..." % llen)
    
    #construct the graph with a minimum of 15 tweets
    # (it is not a good idea to put all retweets in a memory to make a graph
    for i in range(0, min(llen,15)): 		
        popped = json.loads(cfg.red.rpop(tweetId))  
        dict_retweet_set[Retweet(popped)] = popped['rt_time']  
          
    while len(dict_retweet_set.keys()) != 0:
		 older_retweet = dict_retweet_set.smallest()	
		 reverseSearch(older_retweet[0],root)
		 dict_retweet_set.pop_smallest()

    # save the resulting graph on redis 
    graph = json.dumps({'nodes':[n.__dict__ for n in nodes], 'links':links}, indent=2);
    cfg.red.lpush(str(tweetId)+"i", graph)
