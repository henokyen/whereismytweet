from flask import Flask
#from flask import Response
#from flask import json
import redis
app = Flask(__name__)
app.redis = redis.Redis(host='localhost',port=6379,db=0)
from app import views
