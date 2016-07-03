
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
#Therefore, a Neo4j databse was created to hold a simulated twitter like social network. 
#The data is from https://snap.stanford.edu/data/soc-pokec.html
def fecthFriends(user):
  userid = "User_"+str(user['id'])  
  rel = "MATCH (a:User)-[:Follows]->(b:User {name: {S}}) RETURN a.name as name"
  follwed_list = list(cfg.graph.cypher.execute(rel,{"S":userid})) 
  for followed in follwed_list:
	cfg.red.hset("friends:%s" % user['id'], followed.name.strip("User_"), "")    
  
#get all the people this child could have retweeted from
#returns 1 if this child retweeted from any of the existing node that is already in the graph, 0 otherwise
def isFriend(parent,child):
    key = REDIS_FRIENDS_KEY % child['id']
    if (not cfg.red.exists(key)):
          fecthFriends(child) 
    return cfg.red.hexists(key, parent['id']) 

# a new node gets connected to a node that is already in the graph,
# that is a retweeter gets into the graph only after the retweeter that he retweets from is in the graph
def reverseSearch(user,source):
    global nodes, connected
    # discard if duplicate, is that retweet is already part of the graph 
    if user in nodes:
        return 

    # assume node is isolated until parent is found
    parent = None

    # connect user by iterating through already-connected nodes,
    # i.e., find from which other user this current user could have retweeted retweets
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


# Building the graph starts by continuously checking for a Redis cache for a start signal, meaning checking if a user has tweeted
while True:
	if cfg.red.llen('start') == 0:
		time.sleep(30)
	else:
		tweetId = cfg.red.lindex('start',0)
		break


retweetiD = tweetId + "i"

# if there is a retweet graph already constructed for this tweet, fetch that graph and build upon it
if cfg.red.llen(retweetiD)!= 0:
 print "Reading a prevoius retweet graph from Redis keyed at: ",retweetiD
 data = json.loads(cfg.red.lindex(retweetiD,0))
 nodes = data ["nodes"]
 links = data["links"]
 root = nodes[0]
 connected = nodes

#otherwise, start a graph from a scratch, by first retrivig the orignal tweet 
else:            
    root = json.loads(cfg.red.lindex("Orig",0))
    connected.append(root)
    addToGraph(None, root)

# building the re-tweet graph continues until a 'Stop' signal is sensed
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
    # otherwise, pop retweets and put them into the graph  
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


