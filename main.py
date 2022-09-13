from kafka import KafkaConsumer
from kafka import KafkaProducer
from textblob import TextBlob
import os


kafkaServer = os.environ.get('KafkaServer')


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




consumer = KafkaConsumer(
 bootstrap_servers=kafkaServer, api_version=(0,11,5), auto_offset_reset='earliest', group_id=None
)
consumer.subscribe(topics="abcd")

producer = KafkaProducer(bootstrap_servers=[kafkaServer],api_version=(0,11,5))
def produce_sentiment(topic, key, value):
    producer.send( topic,  key = key, value = str(value).encode('utf-8'))



for msg in consumer:
    print(msg)
    temp_tweet = clean_tweet(str(msg))
    str_tweet = ' '.join(str(t) for t in temp_tweet)
    val = sentiment_analyser(str_tweet)
    print(val)
    produce_sentiment("def", msg.key, str(val))
