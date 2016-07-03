
# Defines projct-wide variables and connections Redis, Neorj, and other parameters and methods

import time,os
import datetime
import redis
import tweepy
from py2neo import Graph, authenticate, Node, Relationship
from py2neo.packages.httpstream import http

DATE_FORMAT = "%a %b %d %H:%M:%S +0000 %Y"

# converte the creation times of tweets 
def getTweetTime(data):
        stime = time.strptime(data['created_at'],DATE_FORMAT)
        epochtime = time.mktime(stime)
        return epochtime


# list of users to watch retweets for
userlist = ['255359603',"1","1339835893"]

# connect to a Redis data store 
red = redis.Redis(host='localhost',port=6379,db=0)

# connect to the neo4j grah databse. This database holds a simulated twitter like social network. 
os.environ.get('NEO4J_USERNAME')
url = 'http://localhost:7474'

authenticate(url.strip('http://'), os.environ.get('NEO4J_USERNAME'), os.environ.get('NEO4J_PASSWORD'))
graph = Graph(url+'/db/data/')
graph = Graph()


# set limit of retweets to collect, meaning the re-tweet graph can only consist of retweetlimit number of nodes
retweetlimit = 10000 
