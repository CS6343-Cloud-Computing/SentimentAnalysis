from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.structs import TopicPartition
from textblob import TextBlob
import os


kafkaServer = os.environ.get('KafkaServer')
containerName = os.environ.get('ContainerName')

import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
import string
import re

from nltk.sentiment.vader import SentimentIntensityAnalyzer
nltk.download('vader_lexicon')


sentiments = SentimentIntensityAnalyzer()
stop_words = set(stopwords.words('english'))
def clean_tweet(sentence):
	t= re.split(r"\s+|\\", sentence)
	filtered_t = []

	for i in range(1,len(t)):
		if(t[i].lower() not in stop_words):
			filtered_t.append(re.sub('[^A-Za-z0-9]+', '', t[i].lower()))

	return (filtered_t)
def sentiment_analyser(tweet_str):
	pos_val = [sentiments.polarity_scores(i)["pos"] for i in tweet_str.split(" ")]
	neg_val = [sentiments.polarity_scores(i)["neg"] for i in tweet_str.split(" ")]
	neu_val = [sentiments.polarity_scores(i)["neu"] for i in tweet_str.split(" ")]

	a = sum(pos_val)
	b = sum(neg_val)
	c = sum(neu_val)

	if (a>b) and (a>c):
		return "Positive"
	elif (b>a) and (b>c):
		return "Negative"
	else:
		return "Neutral"



print(kafkaServer.split(","))
consumer = KafkaConsumer(
 bootstrap_servers=kafkaServer, api_version=(0,11,5), auto_offset_reset='earliest', group_id=None
)
consumer.subscribe(topics=containerName)

producer = KafkaProducer(bootstrap_servers=kafkaServer,api_version=(0,11,5))
def produce_sentiment(topic, key, value, headers):
    producer.send( topic,  key = key, value = str(value).encode('utf-8'), headers=headers)



for msg in consumer:
	steps = msg.headers[0][1].decode("utf-8").split("<-->")
	pointer = 1
	# int(msg.headers[1][1].decode("utf-8"))
	temp_tweet = clean_tweet(str(msg))
	str_tweet = ' '.join(str(t) for t in temp_tweet)
	print(str_tweet)
	val = sentiment_analyser(str_tweet)
	print(val)
	outtopic =  "output" if pointer + 1 >= len(steps) else steps[pointer+1]
	produce_sentiment(outtopic, msg.key, str(val), [('flow', "<-->".join(steps).encode()), ('pointer', str(pointer+1).encode())])
