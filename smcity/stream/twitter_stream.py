''' Contains the Twitter stream consuming code. '''

import json

from ConfigParser import ConfigParser
from datetime import datetime
from threading import Thread

from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

from smcity.logging.logger import Logger

logger = Logger(__name__)

class TwitterStreamListener(StreamListener):
    ''' Consumes the twitter stream and uploads the messages into the database. '''
    
    def __init__(self, config, tweet_factory):
        '''
        Constructor.

        @param config Configuration settings for the stream listener. Expected keys:
        Section:     twitter
        Key:         auth_file
        Type:        string
        Description: File containing the authentication details for Twitter
        @paramType ConfigParser
        @param tweet_factory Interface for creating new tweets
        @paramType TweetFactory
        @returns n/a
        '''
        assert tweet_factory is not None, "tweet_factory must not be None"

        # Extract the access credentials from the local configuration
        auth_config = ConfigParser()
        try:
            auth_file = open(config.get('twitter', 'auth_file'), 'r')
            auth_config.readfp(auth_file)
            auth_file.close()
        except:
            message = "Could not open Twitter auth file '" + config.get('twitter', 'auth_file') + "'!"
            logger.exception()
            raise IllegalStateError(message)
        consumer_key = auth_config.get('twitter', 'consumer_key')
        consumer_secret = auth_config.get('twitter', 'consumer_secret')
        access_token = auth_config.get('twitter', 'access_token')
        access_token_secret = auth_config.get('twitter', 'access_token_secret')

        # Set up the stream
        auth_handler = OAuthHandler(consumer_key, consumer_secret)
        auth_handler.set_access_token(access_token, access_token_secret)
        listener = self
        self.stream = Stream(auth_handler, listener)

        self.tweet_factory = tweet_factory       
        self.num_tweets    = 0

    def _consume_stream(self, min_lon, min_lat, max_lon, max_lat):
        '''
        Consumes twitter data tagged inside the provided coordinate box.

        @param min_lon Minimum longitude of the area to consume
        @paramType float
        @param min_lat Minimum latitude of the area to consume
        @paramType float
        @param max_lon Maximum longitude of the area to consume
        @paramType float 
        @param max_lat Maximum latitude of the area to consume
        @paramType float
        @returns n/a
        '''
        assert -180 <= min_lon and min_lon < max_lon and max_lon <= 180, \
            "Expected -180 <= min_lon < max_lon <= 180 got min_lon=%r, max_lon=%r" % (min_lon, max_lon)
        assert -90 <= min_lat and min_lat < max_lat and max_lat <= 90, \
            "Expected -90 <= min_lat < max_lat <= 90, got min_lat=%r, max_lat=%r" % (min_lat, max_lat)

        self.stream.filter(locations=[min_lon, min_lat, max_lon, max_lat])

    def consume_stream(self, min_lon, min_lat, max_lon, max_lat):
        '''
        Consumes twitter data tagged inside the provided coordinate box.

        @param min_lon Minimum longitude of the area to consume
        @paramType float
        @param min_lat Minimum latitude of the area to consume
        @paramType float
        @param max_lon Maximum longitude of the area to consume
        @paramType float 
        @param max_lat Maximum latitude of the area to consume
        @paramType float
        @returns n/a
        '''
        consumer_thread = Thread(target=self._consume_stream, args=(min_lon, min_lat, max_lon, max_lat))
        consumer_thread.daemon = True
        consumer_thread.start()

    def on_data(self, tweet_str):
        '''
        Triggered when a tweet is successfully consumed from the Twitter stream.

        @param tweet_str The tweet to be handled
        @paramType string
        @returns n/a
        '''
        self.num_tweets += 1
        logger.debug("Tweet #%s", self.num_tweets)

        try:
            tweet = json.loads(tweet_str)

            # Extract the interesting parts of the tweet
            id        = tweet['id_str']
            lat       = 0
            lon       = 0
            if 'coordinates' in tweet.keys() and tweet['coordinates'] is not None:
                lat = tweet['coordinates']['coordinates'][0]
                lon = tweet['coordinates']['coordinates'][1]
            elif 'geo' in tweet.keys() and tweet['geo'] is not None:
                lat = tweet['geo']['coordinates'][0]
                lon = tweet['geo']['coordinates'][1]
            else:
                logger.debug("Bad Tweet. Skipping...")
                return # No geotagging, so ignore the tweet
            message   = tweet['text']
            place     = tweet['place']['full_name']
            timestamp = datetime.strptime(tweet['created_at'], '%a %b %d %X +0000 %Y') \
                                .strftime('%Y-%m-%d %X') # TODO Temp +0000

            self.tweet_factory.create_tweet(id, message, place, timestamp, lat, lon)
        except:
            logger.warn("Bad Tweet: %s", tweet_str)
            logger.exception()

    def on_error(self, status):
        '''
        Triggered when an error occurs in the Twitter stream.

        @param status HTTP status code of the error.
        @paramType int
        @returns n/a
        '''
        logger.error("Error occurred in the Twitter stream. Error Code: %s", status)

    def shutdown(self):
        '''
        Stops all active streams.

        @returns n/a
        '''
        self.stream.disconnect()
