''' Units tests for the Twitter stream consumer. '''

import logging

from boto.dynamodb2.table import Table
from ConfigParser import ConfigParser

from smcity.stream.twitter_stream import TwitterStreamListener

logger = logging.getLogger(__name__)

class MockTweetFactory:
    def __init__(self):
        self.tweet = None

    def create_tweet(self, id, message, place, timestamp, lat, lon):
        self.tweet = {
            'id' : id,
            'message' : message,
            'place' : place,
            'timestamp' : timestamp,
            'lat' : lat,
            'lon' : lon
        }

class TestTwitterStreamListener():
    ''' Unit tests for the TwitterStreamListener class. '''

    def setup(self):
        ''' Set up before each test. '''
        logger.info('Setting up the test tweets table...')
        self.table = Table('test_tweets')

        logger.info('Setting up the TwitterStreamListener instance...')
        config = ConfigParser()
        config.add_section('twitter')
        config.set('twitter', 'auth_file', '.twitter')
        self.tweet_factory = MockTweetFactory()
        self.stream_listener = TwitterStreamListener(config, self.tweet_factory, -0.0001, -89, 0.0001, -88)

    def test_on_data(self):
        ''' Tests the on_data function. '''
        logger.info('Setting up the test tweet...')
        tweet = {
            'id_str' : 'id',
            'geo' : {'coordinates' : [0, 1]},
            'text' : 'text',
            'place' : {'full_name' : 'place'},
            'created_at' : 'Mon Jan 01 01:01:01 +0000 2014'
        }

        logger.info('Running the on_data function...')
        self.stream_listener.on_data(tweet)

        logger.info('Validating the results...')
        assert self.tweet_factory.tweet is not None
        assert self.tweet_factory.tweet['id'] == 'id', self.tweet_factory.tweet['id']
        assert self.tweet_factory.tweet['message'] == 'text', self.tweet_factory.tweet['message']
        assert self.tweet_factory.tweet['place'] == 'place', self.tweet_factory.tweet['place']
        assert self.tweet_factory.tweet['lat'] == 0, self.tweet_factory.tweet['lat']
        assert self.tweet_factory.tweet['lon'] == 1, self.tweet_factory.tweet['lon']
        assert self.tweet_factory.tweet['timestamp'] == '2014-01-01 01:01:01', \
            self.tweet_factory.tweet['timestamp']
