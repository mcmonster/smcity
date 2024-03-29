''' Unit tests for the tweets model and factory. '''

import datetime
import logging
import time

from boto.dynamodb2.table import Table
from ConfigParser import ConfigParser

from smcity.models.tweet import TweetFactory, TweetJanitor

logger = logging.getLogger(__name__)

class TestTweetFactory:
    ''' Unit tests for the TweetFactory class. '''

    def setup(self):
        ''' Set up before each test. '''
        logger.info('Setting up the test tweets table...')
        self.table = Table('test_tweets')

        logger.info('Setting up the TweetFactory instance...')
        config = ConfigParser()
        config.add_section('database')
        config.set('database', 'tweets_table', 'test_tweets')
        self.tweet_factory = TweetFactory(config)        

        logger.info('Emptying the contents of the test tweets table...')
        for tweet in self.table.scan():
            tweet.delete()

    def test_create_tweet(self):
        ''' Tests the create_tweet function. '''
        # Try to pass in a missing id
        exception_raised = False
        try:
            self.tweet_factory.create_tweet(None, 'message', 'city', '2013-01-01 01:01:01', 0, 0)
        except:
            exception_raised = True
        assert exception_raised, "Exception was not raised when passing missing id"

        # Try to pass in an illegal message
        exception_raised = False
        try:
            self.tweet_factory.create_tweet('user', None, 'city', '2013-01-01 01:01:01', 0, 0)
        except:
            exception_raised = True
        assert exception_raised, "Exception was not raised when passing illegal message"

        # Try to pass in an illegal city
        exception_raised = False
        try:
            self.tweet_factory.create_tweet('user', 'message', None, '2013-01-01 01:01:01', 0, 0)
        except:
            exception_raised = True
        assert exception_raised, "Exception was not raised when passing illegal place"

        # Try to pass in a missing timestamp
        exception_raised = False
        try:
            self.tweet_factory.create_tweet('user', 'message', 'city', None, 0, 0)
        except:
            exception_raised = True
        assert exception_raised, "Exception was not raised when passing illegal timestamp"

        # Try to pass in a malformed timestamp
        exception_raised = False
        try:
            self.tweet_factory.create_tweet('user', 'message', 'city', '12-01-01 01:01:01', 0, 0)
        except:
            exception_raised = True
        assert exception_raised, "Exception was not raised when passing malformed timestamp"

        # Try to pass a missing lat
        exception_raised = False
        try:
            self.tweet_factory.create_tweet('user', 'message', 'city', '2013-01-01 01:01:01', None, 0)
        except:
            exception_raised = True
        assert exception_raised, "Exception was not raised when passing missing lat"
        
        # Try to pass an underflow lat
        exception_raised = False
        try:
            self.tweet_factory.create_tweet('user', 'message', 'city', '2013-01-01 01:01:01', -90.1, 0)
        except:
            exception_raised = True
        assert exception_raised, "Exception was not raised when passing underflow lat"

        # Try to pass an overflow lat
        exception_raised = False
        try:
            self.tweet_factory.create_tweet('user', 'message', 'city', '2013-01-01 01:01:01', 90.1, 0)
        except:
            exception_raised = True
        assert exception_raised, "Exception was not raised when passing overflow lat"

        # Try to pass a missing lon
        exception_raised = False
        try:
            self.tweet_factory.create_tweet('user', 'message', 'city', '2013-01-01 01:01:01', 0, None)
        except:
            exception_raised = True
        assert exception_raised, "Exception was not raised when passing missing lon"

        # Try to pass an underflow lon
        exception_raised = False
        try:
            self.tweet_factory.create_tweet('user', 'message', 'city', '2013-01-01 01:01:01', 0, -180.1)
        except:
            exception_raised = True
        assert exception_raised, "Exception was not raised when passing underflow lon"

        # Try to pass an overflow lon
        exception_raised = False
        try:
            self.tweet_factory.create_tweet('user', 'message', 'city', '2013-01-01 01:01:01', 0, 180.1)
        except:
            exception_raised = True
        assert exception_raised, "Exception was not raised when passing overflow lon"
        
        # Try to create a valid tweet
        self.tweet_factory.create_tweet('id', 'message', 'city', '2013-01-01 01:01:01', 68.8888, 121.12)
        
        # Verify the tweet record was created
        record = self.table.scan(id__eq='id').next()
        assert record is not None, "Expected record to not be None"
        assert record['place'] == 'city', record['place']
        assert record['lat'] == 688888000, record['lat']
        assert record['lat_copy'] == 688888000, record['lat_copy']
        assert record['lon'] == 1211200000, record['lon']
        assert record['lon_copy'] == 1211200000, record['lon_copy']
        assert record['message'] == 'message', record['message']
        assert record['timestamp'] == '2013-01-01 01:01:01', record['timestamp']

    def test_get_tweets_all(self):
        ''' Tests retrieving all of the tweets. '''
        logger.info('Setting up the test data...')
        self.tweet_factory.create_tweet('user1', 'message1', 'city', '2013-01-01 01:01:01', 0, 0)
        self.tweet_factory.create_tweet('user2', 'message2', 'city', '2013-01-01 01:01:01', 0, 0)
        self.tweet_factory.create_tweet('user3', 'message3', 'city', '2013-01-01 01:01:01', 0, 0)

        logger.info('Trying to retrieve the tweets for the whole city...')
        tweets = self.tweet_factory.get_tweets()

        logger.info('Verifying that all of the messages were retrieved...')
        num_tweets = 0
        for tweet in tweets:
            num_tweets += 1
            assert tweet.message() == 'message' + str(num_tweets), \
                "Expected message to be 'message" + str(num_tweets) + "', got %r" % tweet.message()
        assert num_tweets == 3, "Expected three tweets retrieved, got %r" % num_tweets

    def test_get_tweets_time_restricted(self):
        ''' Tests retrieving all of the tweets that are a certain age or newer. '''
        # Set up the test data
        self.tweet_factory.create_tweet('user1', 'message1', 'city', '2013-01-01 01:01:01', 0, 0)
        self.tweet_factory.create_tweet('user2', 'message2', 'city', '2014-01-01 01:01:01', 0, 0)
        self.tweet_factory.create_tweet('user3', 'message3', 'city', '2014-01-15 01:01:01', 0, 0)

        # Try to retrieve the tweets
        tweets = self.tweet_factory.get_tweets(age_limit='2014-01-01 01:01:01')

        # Verify that the expected messages were retrieved
        num_tweets = 0
        for tweet in tweets:
            num_tweets += 1
            assert tweet.message() == 'message' + str(num_tweets+1), \
                "Expected message to be 'message" + str(num_tweets+1) + "', got %r" % tweet.message()
        assert num_tweets == 2, num_tweets
        
    def test_get_tweets_location_restricted(self):
        ''' Tests retrieving all of the tweets for a city that are within a specified coordinate box. '''
        # Set up the test data
        self.tweet_factory.create_tweet('user1', 'message1', 'city', '2013-01-01 01:01:01', 0, 0)
        self.tweet_factory.create_tweet('user2', 'message2', 'city', '2013-01-01 01:01:02', 1, 1)
        self.tweet_factory.create_tweet('user3', 'message3', 'city', '2013-01-01 01:01:03', 0.5, 0.5)
        self.tweet_factory.create_tweet('user4', 'message4', 'city', '2013-01-01 01:01:04', -1, -1)
        self.tweet_factory.create_tweet('user5', 'message5', 'city', '2013-01-01 01:01:05', -1, 1.5)
        self.tweet_factory.create_tweet('user6', 'message6', 'city', '2013-01-01 01:01:06', 1.5, 0.5)
     
        # Try to retrieve the tweets
        tweets = self.tweet_factory.get_tweets( 
            coordinate_box={'min_lon' : 0, 'min_lat' : 0, 'max_lon' : 1, 'max_lat' : 1}
        )
       
        # Verify that the expected messages were retrieved
        num_tweets = 0
        for tweet in tweets:
            num_tweets += 1
            print "Got:", tweet.message()
            assert tweet.lat() >= 0 and tweet.lat() <= 1, tweet.lat()
            assert tweet.lon() >= 0 and tweet.lon() <= 1, tweet.lon()
        assert num_tweets == 3, num_tweets

    def test_get_tweets_location_and_time_restricted(self):
        ''' Tests retrieving all of the tweets for a city that are within a specified coordinate box and time. '''
        logger.info('Setting up test data...')
        self.tweet_factory.create_tweet('user1', 'message1', 'city', '2013-01-01 01:01:03', 0, 0)
        self.tweet_factory.create_tweet('user2', 'message2', 'city', '2013-01-01 01:01:02', 1, 1)
        self.tweet_factory.create_tweet('user3', 'message3', 'city', '2013-01-01 01:01:01', 0.5, 0.5)
        self.tweet_factory.create_tweet('user4', 'message4', 'city', '2013-01-01 01:01:04', -1, -1)
        self.tweet_factory.create_tweet('user5', 'message5', 'city', '2013-01-01 01:01:05', -1, 1.5)
        self.tweet_factory.create_tweet('user6', 'message6', 'city', '2013-01-01 01:01:06', 1.5, 0.5)

        logger.info('Trying to retrieve the tweets...')
        tweets = self.tweet_factory.get_tweets(
            coordinate_box={'min_lon' : 0, 'min_lat' : 0, 'max_lon' : 1, 'max_lat' : 1}, 
            age_limit='2013-01-01 01:01:02'
        )
        
        logger.info('Verifying that the expected messages were retrieved...')
        num_tweets = 0
        for tweet in tweets:
            num_tweets += 1
            assert tweet.message() == 'message' + str(num_tweets), tweet.message()
        assert num_tweets == 2, num_tweets

class TestTweetJanitor:
    ''' Unit tests for the TweetJanitor class. '''

    def setup(self):
        ''' Set up before each test. '''
        logger.info('Setting up the test tweets table...')
        self.table = Table('test_tweets')

        logger.info('Setting up the TweetFactory instance...')
        config = ConfigParser()
        config.add_section('database')
        config.set('database', 'max_tweet_age', '1')
        config.set('database', 'tweets_table', 'test_tweets')
        self.tweet_factory = TweetFactory(config)

        logger.info('Setting up the TweetJanitor instance...')
        self.tweet_janitor = TweetJanitor(config)

        logger.info('Emptying the contents of the test tweets table...')
        for tweet in self.table.scan():
            tweet.delete()

    def teardown(self):
        self.tweet_janitor.shutdown()

    def test_maintain_tweets(self):
        ''' Tests the maintain_tweets routine. '''
        logger.info('Setting up test data...')
        self.tweet_factory.create_tweet('user1', 'message', 'city', '2013-01-01 01:01:01', 0, 0)
        self.tweet_factory.create_tweet('user2', 'message', 'city', '2013-01-10 01:01:01', 0, 0)
        self.tweet_factory.create_tweet('user3', 'message', 'city', '2013-01-30 01:01:01', 0, 0)
        self.tweet_factory.create_tweet('user4', 'message', 'city',
            datetime.datetime.fromtimestamp(time.time()-3601).strftime('%Y-%m-%d %H:%M:%S'), 0, 0)
        self.tweet_factory.create_tweet('user5', 'message', 'city', 
            datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), 0, 0)
        self.tweet_factory.create_tweet('user6', 'message', 'city', '2020-01-01 01:01:01', 0, 0)

        logger.info('Spinning up the clean up routine...')
        self.tweet_janitor.maintain_tweets()
        time.sleep(5) # Wait a second to let the janitor do its work

        logger.info('Verify that the tweets were deleted as expected...')
        expected_tweets = ['user5', 'user6']
        current_tweet = 0
        tweets = self.tweet_factory.get_tweets('city')
        for tweet in tweets:
            assert tweet.id() == expected_tweets[current_tweet], tweet.id()
            current_tweet += 1
