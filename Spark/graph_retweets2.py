
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

# unconnected (retweeters):
# just a temporary container for redis dicts for final step
unconnected = []

# connected (retweeters):
# conduct search through connected RT'ers while adding new edges
connected = []

# result graph: 2 lists
links = []
nodes = []

# Adds nodes and links between them to the graph
def addToGraph (parent, child):
    global links
    if (child):
          nodes.append(child)
	  if (parent):
	      links.append({'source':getNodeIndex(parent), 
                          'target':getNodeIndex(child)})

# links are made between indices of nodes
def getNodeIndex (user):
    global nodes
    for i in range(len(nodes)):         
        if (user['id'] == nodes[i]['id']):
            return i
    return -1

#form the Neo4j database, fetch users who are followed by this user. 
# Note getting this information from Twitter is very time consuming. 
#Therefore, a Neorj bd is created to hold the simulated twitter like social network: https://snap.stanford.edu/data/soc-pokec.html
def fecthFriends(user):
  userid = "User_"+str(user['id'])  
  rel = "MATCH (a:User)-[:Follows]->(b:User {name: {S}}) RETURN a.name as name"
  follwed_list = list(cfg.graph.cypher.execute(rel,{"S":userid})) 
  for followed in follwed_list:
	cfg.red.hset("friends:%s" % user['id'], followed.name.strip("User_"), "")    
  
#get all the people this child could have retweeted from
#returns 1 if this child retweeted from any the existing node, 0 otherwise
def isFriend(parent,child):
    key = REDIS_FRIENDS_KEY % child['id']
    if (not cfg.red.exists(key)):
          fecthFriends(child) 
    return cfg.red.hexists(key, parent['id']) 

# a new node gets connected to a node that is already in the graph,
# that is a retweeter gets into the graph only after the retweeter that he retweets from is in the graph
def reverseSearch(user,source):
# user: userdict from Redis
    global nodes, connected
    # discard if duplicate, is that retweet is already part of the graph 
    if user in nodes:
        return 

    # assume node is isolated until parent is found
    parent = None

    # connect user by iterating through already-connected nodes, i.e., find from which other user this current user could have retweeted retweets
    for existing in connected:
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


# Main Program

# optional: supply tweetId to pick up from redis, if it is still in memmory otherwise read it from disk
if len(sys.argv) == 2:
    tweetId = sys.argv[1]

#if tweetId is supplied, do not wait on Redis signal. Mostly, it is hard to know the id of a certain tweet
if len(sys.argv) == 2:
    tweetId = sys.argv[1]
else:
    # continuously check Redis for start signal, meaning wait for a user to tweet
    while True:
        if cfg.red.llen('start') == 0:
            time.sleep(30)
        else:
            tweetId = cfg.red.lindex('start',0)
            break


retweetiD = tweetId + "i"
if cfg.red.llen(retweetiD)!= 0:
 print "Reading a prevoius retweet graph from Redis keyed at: ",retweetiD
 data = json.loads(cfg.red.lindex(retweetiD,0))
 nodes = data ["nodes"]
 links = data["links"]
 root = nodes[0]
 connected = nodes

else:        
    #retrive the orignal tweet (the one a retweet graph is going to be constructed)
    root = json.loads(cfg.red.lindex("Orig",0))
    connected.append(root)
    addToGraph(None, root)


# iterate over CURRENT list of unconnected retweets from Redis
quit = 0
while (quit == 0):

    # check for "Stop" signal, i.e., if enough retweet has been collected. If so, lpop it
    if (cfg.red.lindex(tweetId,0) == "Stop"): 
        cfg.red.lpop(tweetId)
        quit = 1

    # if the list is empty, sleep and wait for retweets to arrive 
    llen = cfg.red.llen(tweetId) 
    if llen == 0:
        print("Retweet queue is empty, sleeping for 40 seconds.")
        time.sleep(40)
        continue
    # otherwise, process the retweets and form the graph off them 
    print("Forming a graph with %s retweets retrieved from Redis..." % llen)
    for i in range(0, min(llen,30)):
        popped = json.loads(cfg.red.rpop(tweetId))    ]  
        unconnected.append(popped)
    for retweeter in unconnected:
          reverseSearch(retweeter,root)

    
    graph = json.dumps({'nodes':nodes ,'links':links}, indent=2);
    cfg.red.lpush (str(tweetId)+"i", graph)
    
    # clear unconnected so we don't re-attach 
    unconnected = []


