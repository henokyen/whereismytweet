#!/usr/bin/python3
#
# Builds the retweet graph from the retweets stored in Redis
#
# Can pick up from a partially-populated graph by specifying the original
# tweet ID 


import cfg
import sys
import tweepy
import time
import json
import redis
import os

# unconnected (retweeters):
# just a temporary container for redis dicts for final step
unconnected = []

# connected (retweeters):
# conduct search through connected RT'ers while adding new edges
connected = []

# result graph: 2 lists
links = []
nodes = []

#----------------------------------------
def extractDict(status):
# status: tweepy.models.Status
#----------------------------------------
    userdict = dict({'id':status['user']['id'],
                     'screen_name':status['user']['screen_name'],
                     'followers_count':status['user']['followers_count'],
                     'rt_time':cfg.dt_to_epoch(status['created_at'])})
    
    return userdict 

#----------------------------------------
def addToGraph (parent, child):
# parent: userdict
# child: userdict
#----------------------------------------
    global links

    if (child):
        nodes.append(child)        
        if (parent):           
            links.append({'source':getNodeIndex(parent), 
                          'target':getNodeIndex(child)})
#----------------------------------------
def getNodeIndex (user):
# node: userdict
#----------------------------------------
    global nodes

    for i in range(len(nodes)): 
        
        if (user['id'] == nodes[i]['id']):
            return i
    return -1

# form the neo4j db, fecth users who are followed by user
def fecthFriends(user):
  userid = "User_"+str(user['id'])  
  rel = "MATCH (a:User)-[:Follows]->(b:User {name: {S}}) RETURN a.name as name"
  follwed_list = list(cfg.graph.cypher.execute(rel,{"S":userid})) 
  for followed in follwed_list:
	cfg.red.hset("friends:%s" % user['id'], followed.name.strip("User_"), "")    
  
#----------------------------------------
def isFriend(parent,child):
# parent, child, root: userdict 
#----------------------------------------
   
    #print "the child is ", child 
    # if child hasn't been crawled yet, get data from Neo4j. Note getting this information from Twitter is very time consuming
    key = REDIS_FRIENDS_KEY % child['id']
    if (not cfg.red.exists(key)):
          fecthFriends(child) # get all the people this child is followed
    return cfg.red.hexists(key, parent['id']) #returns if existing node is being followed by a retweeter

#----------------------------------------

def reverseSearch(user,source):
# user: userdict from Redis
#----------------------------------------

    global nodes, connected, api

    # discard if duplicate
    if user in nodes:
        return 

    # assume node is isolated until parent is found
    parent = None

    # connect user by iterating through already-connected nodes
    for existing in connected:

        # NOTE: since tweets are caught chronologically, child's
        # parent is guaranteed to be in the (existing) graph
        if isFriend(existing,user):
            parent = existing
            break

    if parent is not None: 
        print ("    New edge: %s <=== %s" % (parent['screen_name'], user['screen_name']))
        addToGraph(parent, user)
    else:
        print ("    User %s is isolated" % user['screen_name'])
        addToGraph(None, user)

    # retweeter has been connected
    connected.append(user)

#----------------------------------------
# Main Program
#----------------------------------------

# optional: supply tweetId to pick up from redis, if it is still in memmory otherwise read it from disk
if len(sys.argv) == 2:
    tweetId = sys.argv[1]

# Connect to Twitter 
REDIS_FRIENDS_KEY = "friends:%s" 

#if tweetId is supplied, do not wait on Redis signal. Mostly, it is hard to know the id of a certain tweet
if len(sys.argv) == 2:
    tweetId = sys.argv[1]
else:
    # continuously check Redis for start signal, meaning for a user to tweet
    while True:
        if cfg.red.llen('start') == 0:
            time.sleep(30)
        else:
            tweetId = cfg.red.lindex('start',0)
            break

# set JSON filename for this tweet
filename = "../data/streaming/%s.json" % tweetId

# if JSON file does not exist, this is the first pass, the initialize the graph
retweetiD = tweetId + "i"
if cfg.red.llen(retweetiD)!=0:
 print "Reading a prevoius retweet graph from Redis keyed at: ",retweetiD
 data = json.loads(cfg.red.lindex(retweetiD,0))
 nodes = data["nodes"]
 links = data["links"]
 # set the root
 root = nodes[0]
 # populate connected
 connected = nodes

else:
    root = cfg.red.lindex("Orig",0) # get the original tweet from redis     
    rot = json.loads(root)
    connected.append(rot)
    addToGraph(None, rot)

# NOTE: BFS search through retweeters
# will connect isolated nodes at the end

# iterate over CURRENT list of unconnected retweets from Redis
quit = 0
while (quit == 0):

    # check for "Stop" signal, if present, lpop it
    if (cfg.red.lindex(tweetId,0) == "Stop"): # i.e., if there is still retweets coming up
        cfg.red.lpop(tweetId)
        quit = 1

    # if the list is empty, sleep
    llen = cfg.red.llen(tweetId) # llen is the number of people who retweets the tweet with tweetId
    if llen == 0:
        print("Retweet queue is empty, sleeping for 30 seconds.")
        time.sleep(30)
        continue
   
    # if not empty, pop from Redis onto unconnected 
    # retrieve minimum of llen, 15

    if quit == 1:
        n = llen
    else:
        n = min(llen,15) # check with 15 retweeters 
    # process these from the redis data store instead of ...
    for i in range(n):
        popped = cfg.red.rpop(tweetId)
        print popped
        unconnected.append(json.loads(popped))
    print("Forming a graph with %s retweets retrieved from Redis..." % n)
    
    # connect the graph
    for retweeter in unconnected:
        reverseSearch(retweeter,root)
    graph = json.dumps({'nodes':nodes, 'links':links}, indent=2);
    cfg.red.lpush (str(tweetId)+"i", graph)
    
    # clear unconnected so we don't re-attach 
    unconnected = []


