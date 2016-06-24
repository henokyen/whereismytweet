#!/usr/bin/python3
# defines and declared projec wide used varaible 

import time,os
import datetime
import redis
import tweepy
from py2neo import Graph, authenticate, Node, Relationship
from py2neo.packages.httpstream import http

def unix_time(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return delta.total_seconds()

def dt_to_epoch(dt):
    return unix_time(dt) * 1000.0

CONSUMER_KEY = 'XXX'
CONSUMER_SECRET = 'XXX'
ACCESS_TOKEN = 'XXX'
ACCESS_SECRET = 'XXX'

# list of users to watch retweets for
userlist = ['255359603',"1","1339835893"]

# set time interval for shutting down stream
# if the next RT takes longer than this to arrive, exit the stream_listen
tlimit = 150000 

# set limit of retweets to collect
limit = 10000
#connect to the redis data store  
red = redis.Redis(host='localhost',port=6379,db=0)

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
api = tweepy.API(auth)

# connect to the neo4j grah db, which holds a simulated twitter like social network. 

os.environ.get('NEO4J_USERNAME')
url = 'http://localhost:7474'
authenticate(url.strip('http://'), os.environ.get('NEO4J_USERNAME'), os.environ.get('NEO4J_PASSWORD'))
graph = Graph(url+'/db/data/')

graph = Graph()
