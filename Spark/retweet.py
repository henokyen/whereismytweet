"""
Because re-tweets are used as keys in a dictionary, I created a Retweet class.
the class defines the __hash__() __eq__() methods, i.e., for retweet objects that __eq__ compares equal, __hash__ returns the same hash value.
"""
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
	  
