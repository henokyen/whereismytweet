"""
Defines retweets as objects.
"""

import json
class Retweet (object):
  def __init__(self, retweet):
	  self.id = retweet['id']
	  self.screen_name = retweet['screen_name']
	  self.followers_count = retweet['followers_count']
	  self.color = retweet['color']
          self.size = retweet['size']
  def __eq__ (self, other):
	  if (self.id) == other.id:return True
	  else: return False 
  def __hash__(self):
	  return hash(self.id)
	  
