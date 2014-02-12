#!/usr/local/bin/python python

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


#Tweepy Streaming API Listener
class StdOutListener(tweepy.StreamListener):
	''' Handles data received from the stream and saves it onto MongoDB. '''

	def on_connect(self):
		mongo = pymongo.MongoClient(dbparams['mongodb']['host'])
		db =  mongo[dbparams['mongodb']['name']]
		self.collection = db[dbparams['mongodb']['collection']]

	def on_data(self, data):
		# Prints the text of the tweet
		try:
			tweets = json.loads(data)
			print tweets['text']
			#Timestamp on the tweet
			dt = parser.parse(tweets['created_at'])
			tweets['timestamp'] = float(calendar.timegm(dt.utctimetuple())*1000)
			#Insert
			self.collection.insert(tweets)
			return True
		except Exception, e:
			print sys.stderr, ' Encountered Exception: ' , e
			pass		

	def on_error(self, status_code):
		print('Got an error with status code: ' + str(status_code))
		return True # To continue listening

	def on_timeout(self):
		print('Timeout...')
		return True # To continue listening
 


if __name__ == '__main__':



	#Run command line
	#python ~/Dropbox/code/utilities/tweepy_mongodb_update.py --dbparams '/Users/denizzorlu/Dropbox/code/powerpoetry/data/dbparams.json' --inputlist '/Users/denizzorlu/Dropbox/code/powerpoetry/data/twitter_keywords.json'
	try:
		opts,args = getopt.getopt(sys.argv[1:],'d:i', ['dbparams=','inputlist='])
	except getopt.GetoptError:
		pass
	#Parse Arguments
	for opt, arg in opts:
		if opt in ('-d','--dbparams'):
			dbpath = arg
		elif opt in ('-l','--inputlist'):
			inputlist = arg

	#Read Input File
	print 'Starting processing'
	#Keywords
	with open('%s'% (inputlist)) as f: d  =  json.loads(f.read())
	keywords = d['keywords']
	print 'Keywords to be searched: '
	print keywords

	#DBParams
	with open('%s'% (dbpath)) as f: dbparams  =  json.loads(f.read())

	#Authorization
	auth = tweepy.OAuthHandler(dbparams['twitter']['consumer_key'], dbparams['twitter']['consumer_secret'])
	auth.set_access_token(dbparams['twitter']['oauth_token'], dbparams['twitter']['oauth_secret'])

		#Set up Steram
	stream = tweepy.streaming.Stream(auth, StdOutListener())
	stream.filter(track=keywords)


