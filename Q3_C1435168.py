# -*- coding: utf-8 -*-

import tweepy
import json
import sqlite3
from textblob import TextBlob
from urllib3.exceptions import ProtocolError

# Storing tokens given by Twitter to access the API
access_token = '790516966733873152-vrZUd4Lrucdk0Y2y1ztmqj1Nk4ZOQHZ'
access_token_secret = 'IXtMntWUOAgoyKGr1kZRoFOhX85zvtwLWFeMaeNkxqggQ'
consumer_key = 'Jc36um9LnPYgXbTCK6zU7hlUM'
consumer_secret = 'YDDhm9aoHmS70JXMiDzyGEfA5P3QURJG6y4yl86vWc5smXjDUG'

# Create a sqlite database and connect to it
conn = sqlite3.connect('Q3_sqlite_C1435168.sqlite')
c = conn.cursor()

# Create a table in the database called tweets

# tb_create = """CREATE TABLE tweets 
# 			(created_at TEXT, 
# 			tweet_id TEXT, 
# 			tweet_text TEXT, 
# 			user_location TEXT, 
# 			geo_coordinates TEXT, 
# 			user_followers_count TEXT, 
# 			user_friends_count TEXT, 
# 			sentiment_analysis TEXT)"""
# 
# c.execute(tb_create)
# conn.commit()
	

count = 0

# Creates a StreamListener class using tweepy
class StreamListener(tweepy.StreamListener):
    
	def on_status(self, status):
		print(status)

    # Prints the error if program fails
	def on_error(self, status_code):
		print(status_code)

	def on_data(self, data):
        
		all_data				= json.loads(data)
        
        # Takes relevant data from the data being streamed
		created_at 				= all_data['created_at']
		tweet_id 				= all_data['id_str']
		tweet_text 				= all_data['text']
		user_location 			= all_data['user']['location']
        
        # Grabs geo coordinates from Tweet data if the user has enabled it
		if all_data['coordinates'] == None:
			geo_coordinates = all_data['coordinates']
		else:
			geo_coord = all_data['coordinates']['coordinates']
			geo_coordinates 	= str([geo_coord[1], geo_coord[0]]).replace('[','').replace(']','')
		

		user_followers_count 	= all_data['user']['followers_count']
		user_friends_count 		= all_data['user']['friends_count']

        # Using TextBlob for sentiment analysis of the Tweets
		tb = TextBlob(tweet_text.strip())

		global count

		count += 1
		senti = 0

		sentiment_analysis 		= senti + tb.sentiment.polarity

        # Prints the ID of every Tweet being streamed, just so you can see
        # that the program is working and Tweets are being streamed.
		print(tweet_id)
		
        # Inserts the relevant data into the created sqlite database.
		c.execute("""INSERT INTO tweets 
			(created_at, 
			tweet_id, 
			tweet_text, 
			user_location, 
			geo_coordinates, 
			user_followers_count, 
			user_friends_count, 
			sentiment_analysis)
			

			VALUES (?,?,?,?,?,?,?,?)""",
            # Linking the variables to the relevant table columns
			(	created_at, 
				tweet_id, 
				tweet_text, 
				user_location, 
				geo_coordinates, 
				user_followers_count, 
				user_friends_count, 
				sentiment_analysis))

		conn.commit()

# Authorization to use the Twitter API
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

stream_listener = StreamListener()
stream = tweepy.Stream(auth = api.auth, listener = stream_listener)

# Searches Twitter for Tweets containing a keyword, while ignoring 
# Protocol Errors which stops the program streaming
while True:
	try:
        # Searches for Tweets containing the keyword 'Trump' 
		stream.filter(track=['Trump'], languages = ['en'])
	except (ProtocolError, AttributeError):
		continue