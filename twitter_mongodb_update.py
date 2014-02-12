#!/usr/local/bin/python python

import twitter
import tweepy
import time
import pymongo
import json
from dateutil import parser
import calendar
import getopt
import sys
from datetime import datetime
import pandas as pd


COUNT = 100

"Python Twitter API to retrieve REST or STREAM"


def twitter_stream_mongodb_update(keys,search):
	#Continously updates the collections in mongodb AWS given the search terms.
	#oAuth is a dictionary containing twitter authorization keys. 

	#Using MongoDB
	connection = pymongo.MongoClient(keys['mongodb']['host'])
	dbtm = connection[keys['mongodb']['name']]
	tweets = dbtm[keys['mongodb']['name']]
	#Connect to Twitter Stream API
	oAuth = keys['twitter']
	twitter_stream = twitter.stream.TwitterStream(domain='stream.twitter.com',api_version='1.1',auth=twitter.oauth.OAuth(oAuth['oauth_token'],oAuth['oauth_secret'],oAuth['consumer_key'],oAuth['consumer_secret']))

	while True:
		try:
			#BUild the iterator
			iterator = twitter_stream.statuses.filter(track=search)
			#Insert into MongoDB
			for tweet in iterator:
				tweet['_id'] = tweet['id_str']
				#Date - First to datetime, than to UNIX timestamp in miliseconds. 
				dt = parser.parse(tweet['created_at'])
				uts = float(calendar.timegm(dt.utctimetuple())*1000)
				tweet['timestamp'] = uts
				try:
					tweets.insert(tweet)
					print tweet['text'], tweet['_id'], tweet['created_at']
				except:
					print 'pass'
		except:
			#wait
			print 'Reconnecting to Twitter stream'
			time.sleep(30)

def twitter_search_mongodb_update(keys,search ,count = COUNT):
	#Continously updates the collections in mongodb localhost given the search term.
	#Search term does not include hashtag.
	#oAuth is a dictionary containing twitter authorization keys. 

	#Using MongoDB
	connection = pymongo.MongoClient(keys['mongodb']['host'])
	dbtm = connection[keys['mongodb']['name']]
	tweets = dbtm.tweets
	#Connect to Twitter API
	oAuth = keys['twitter']
	t = twitter.Twitter(domain='api.twitter.com', api_version='1.1', auth=twitter.oauth.OAuth(oAuth['oauth_token'],oAuth['oauth_secret'],oAuth['consumer_key'],oAuth['consumer_secret']))
	#Loop to collect tweets. 
	#MongoDB
	while True:
		try:
			for term in search:
				print term
				#term = '#'+term
				tw = t.search.tweets(count=count,q=term,show_user=True)
				statuses = tw['statuses']
				for status in statuses:
					try:	
						#Explicitly insert with id_str 
						status['_id'] = status['id_str']
						#Date - First to datetime, than to UNIX timestamp in miliseconds. 
						dt = parser.parse(status['created_at'])
						uts = calendar.timegm(dt.utctimetuple())*1000
						status['timestamp'] = uts
						tweets.insert(status)
						print status['text'], status['created_at'], status['id_str']
					except:
						print status['id_str'], ' exists'
				#wait
				time.sleep(30)
		except:
			print 'Error. Prob Hit Rate Limit. Wait 10 minutes'
			time.sleep(600)

if __name__ == '__main__':



	#Run command line
	#python ~/Dropbox/code/crisismonitor/twitter_mongodb_update.py --keys '/Users/denizzorlu/Dropbox/code/powerpoetry/data/dbparams.json' --inputlist '['micropoetry']'
	#ython ~/Dropbox/code/crisismonitor/twitter_mongodb_update.py --keys '/Users/denizzorlu/Dropbox/code/powerpoetry/data/dbparams.json' --inputlist '/Users/denizzorlu/Dropbox/code/powerpoetry/data/keywords.json'

	try:
		opts,args = getopt.getopt(sys.argv[1:],'k:i', ['keys=','inputlist='])
	except getopt.GetoptError:
		pass
	#Parse Arguments
	for opt, arg in opts:
		if opt in ('-k','--keys'):
			dbpath = arg
		elif opt in ('-i','--inputlist'):
			inputlist = arg
	#Read Input File
	print 'Starting processing'
	#Keywords
	with open('%s'% (inputlist)) as f: d  =  json.loads(f.read())
	terms = d['keywords']
	print terms
	#Keys
	with open('%s'% (dbpath)) as f: dbparams  =  json.loads(f.read())

	twitter_search_mongodb_update(dbparams,terms,COUNT)





