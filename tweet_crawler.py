#!/usr/bin/env python

import re
import tweepy
import datetime
from pymongo import MongoClient
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.tokenize import sent_tokenize, word_tokenize

# NLTK configurations
porter_stemmer = PorterStemmer()
stop_words = stopwords.words("english")

# Twitter API credentials
consumer_key = ""
consumer_secret = ""
access_key = ""
access_secret = ""

# List of twitter accounts to follow
screen_name_list = ["nytimes", "thetimes"]

# Tokenize and filter tweet
def tokenize_filter(text):
	# First tokenize by sentence, then by word to ensure that punctuation is caught as it's own token
	tokens = [word.lower() for sent in sent_tokenize(text) for word in word_tokenize(sent)]
	filtered_tokens = []
	# Filter out any tokens not containing letters (e.g., numeric tokens, raw punctuation)
	for token in tokens:
		if re.search('[a-zA-Z]', token):
			filtered_tokens.append(token)
	return filtered_tokens
	
# Remove stopwords from string
def remove_stopwords(tokens):
	return [word for word in tokens if word not in stop_words]

# Stem words in a sentence
def stem_text(tokens):
	return [porter_stemmer.stem(word) for word in tokens]

# return tweet text if original, and source_text if retweet (remove unnecessery RT, and author mention)
def get_tweet_text(tweet):
	tweet_text = tweet['text']
	if tweet['is_retweet']:
		tweet_text = tweet['source_text']
	return tweet_text

# Encode, remove extra charachters, urls, stop words, and do stemming
def process_tweet_text(tweet):
	tweet_text = get_tweet_text(tweet)
	tweet_text = tweet_text.decode('utf-8').replace('\n','')
	tweet_text = re.sub(r"http\S+", "", tweet_text)
	tweet_tokens = tokenize_filter(tweet_text)
	tweet_tokens = remove_stopwords(tweet_tokens)
	tweet_tokens = stem_text(tweet_tokens)
	tweet_text = ' '.join(tweet_tokens)
	return tweet_text

# Initialize Twitter API using Tweepy
def init_twitter_api():
	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_key, access_secret)
	return tweepy.API(auth)

# For each screen_name get users timeline and return all the recent 200 tweets
def get_all_tweets(screen_name_list):
	api = init_twitter_api()
	all_tweets = []
	for screen_name in screen_name_list:
		new_tweets = api.user_timeline(screen_name = screen_name, count=200)
		all_tweets.extend(new_tweets)

	return all_tweets

# Parse tweet entity, return entity property value by the key
def parse_tweet_entity(tweet, entity_name, key):
	value = ""
	if entity_name in tweet.entities:
		if (len(tweet.entities[entity_name]) > 0):
			value_list = []
			for entity in tweet.entities[entity_name]:
				value_list.append(entity[key])
			value = ",".join(value_list)

	return value

# Process tweets to be ready for saving in DB
def process_tweets(current_time, all_tweets):
	processed_tweets = []
	for tweet in all_tweets:
		processed_tweet = dict()

		processed_tweet['tweet_id'] = tweet.id
		processed_tweet['screen_name'] = tweet.user.screen_name
		processed_tweet['is_retweet'] = False
		processed_tweet['created_at'] = tweet.created_at
		processed_tweet['tweet_age_mins'] = (current_time - tweet.created_at).total_seconds() / float(60)
		processed_tweet['text'] = tweet.text.encode("utf-8")

		if hasattr(tweet, 'retweeted_status'):
			processed_tweet['is_retweet'] = True
			processed_tweet['source_id'] = tweet.retweeted_status.id
			processed_tweet['source_text'] = tweet.retweeted_status.text.encode("utf-8")
			tweet = tweet.retweeted_status

		processed_tweet['retweet_count'] = tweet.retweet_count
		processed_tweet['favorite_count'] = tweet.favorite_count

		processed_tweet['user_mentions'] = parse_tweet_entity(tweet, "user_mentions", "screen_name")
		processed_tweet['hashtags'] = parse_tweet_entity(tweet, "hashtags", "text")
		processed_tweet['urls'] = parse_tweet_entity(tweet, "urls", "expanded_url")
		processed_tweet['media'] = parse_tweet_entity(tweet, "media", "media_url")
		processed_tweet['processed_tweet_text'] = process_tweet_text(processed_tweet)

		processed_tweets.append(processed_tweet)

	return processed_tweets

# Create/connect to DB
def init_db():
	client = MongoClient()
	db = client.tweetsDB
	return db.tweets

# Save tweets in DB
def save_tweets(processed_tweets):
	tweets_collection = init_db()
	bulk = tweets_collection.initialize_unordered_bulk_op()
	for tweet in processed_tweets:
		bulk.find({'tweet_id': tweet['tweet_id']}).upsert().replace_one(tweet)

	bulk.execute()

# Script entry point
if __name__ == '__main__':
	current_time = datetime.datetime.utcnow()
	all_tweets = get_all_tweets(screen_name_list)
	processed_tweets = process_tweets(current_time, all_tweets)
	save_tweets(processed_tweets)




