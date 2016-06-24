from app import app
from flask import Response,render_template, request,redirect,url_for,abort,session
from flask import json
app.config['SECRET_KEY'] = '^&YHTT$$';
@app.route('/')
def home():
  return render_template('index.html')

@app.route('/grapht',methods=['POST'])
def grapht():
  retweetiD = str(request.form ['tweetID']) + "i"
  print retweetiD
  jresp = json.loads(app.redis.lindex(retweetiD,0))
  
  #print "the jason ", jresp
  #jresp = Response(data,status = 200, mimetype='application/json')  
  return render_template('jason.html', nds = json.dumps(jresp["nodes"]), lnk = json.dumps(jresp["links"]))# nds = jresp["nodes"],  lnk = jresp["links"])

@app.route('/jason')
def jason ():
 if not 'tweetID' in session:   
   return abort(403)
 return render_template('jason.html', tweetID = session['tweetID'])

@app.route('/index')
def index():
  return app.root_path
  return "Hello, Flask from starbuks. Ternado running!!"

@app.route('/retweets/<tweetID>')
def get_retweets(tweetID):
  retweetiD = str(tweetID) +"i"
  print retweetiD 
  data = app.redis.rpop(retweetiD)
  resp = Response(data,status = 200, mimetype='application/json')
  return resp

