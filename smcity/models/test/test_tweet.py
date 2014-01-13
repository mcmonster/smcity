''' Unit tests for the tweets model and factory. '''

import logging

from boto.dynamodb2.table import Table
from ConfigParser import ConfigParser

from smcity.models.tweet import TweetFactory

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
        # Try to pass in a missing user
        exception_raised = False
        try:
            self.tweet_factory.create_tweet(None, 'message', 'city', '2013-01-01 01:01:01', 0, 0)
        except:
            exception_raised = True
        assert exception_raised, "Exception was not raised when passing missing user"

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
        assert exception_raised, "Exception was not raised when passing illegal city"

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
        self.tweet_factory.create_tweet('user', 'message', 'city', '2013-01-01 01:01:01', 0, 0)
        
        # Verify the tweet record was created
        record = self.table.scan(user__eq='user').next()
        assert record is not None, "Expected record to not be None"
        assert record['city'] == 'city', record['city']
        assert record['lat'] == 0, record['lat']
        assert record['lat_copy'] == 0, record['lat_copy']
        assert record['lon'] == 0, record['lon']
        assert record['lon_copy'] == 0, record['lon_copy']
        assert record['message'] == 'message', record['message']
        assert record['timestamp'] == '2013-01-01 01:01:01', record['timestamp']

    def test_get_tweets_whole_city(self):
        ''' Tests retrieving all of the tweets for a city. '''
        logger.info('Setting up the test data...')
        self.tweet_factory.create_tweet('user1', 'message1', 'city', '2013-01-01 01:01:01', 0, 0)
        self.tweet_factory.create_tweet('user2', 'message2', 'city', '2013-01-01 01:01:01', 0, 0)
        self.tweet_factory.create_tweet('user3', 'message3', 'city', '2013-01-01 01:01:01', 0, 0)

        logger.info('Trying to retrieve the tweets for the whole city...')
        tweets = self.tweet_factory.get_tweets('city')

        logger.info('Verifying that all of the messages were retrieved...')
        num_tweets = 0
        for tweet in tweets:
            num_tweets += 1
            assert tweet.message() == 'message' + str(num_tweets), \
                "Expected message to be 'message" + str(num_tweets) + "', got %r" % tweet.message()
        assert num_tweets == 3, "Expected three tweets retrieved, got %r" % num_tweets

    def test_get_tweets_time_restricted(self):
        ''' Tests retrieving all of the tweets for a city that are a certain age or newer. '''
        # Set up the test data
        self.tweet_factory.create_tweet('user1', 'message1', 'city', '2013-01-01 01:01:01', 0, 0)
        self.tweet_factory.create_tweet('user2', 'message2', 'city', '2014-01-01 01:01:01', 0, 0)
        self.tweet_factory.create_tweet('user3', 'message3', 'city', '2014-01-15 01:01:01', 0, 0)

        # Try to retrieve the tweets
        tweets = self.tweet_factory.get_tweets('city', age_limit='2014-01-01 01:01:01')

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
        tweets = self.tweet_factory.get_tweets('city', 
            coordinate_box={'min_lon' : 0, 'min_lat' : 0, 'max_lon' : 1, 'max_lat' : 1})
       
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
        tweets = self.tweet_factory.get_tweets('city', 
            coordinate_box={'min_lon' : 0, 'min_lat' : 0, 'max_lon' : 1, 'max_lat' : 1}, 
            age_limit='2013-01-01 01:01:02'
        )
        
        logger.info('Verifying that the expected messages were retrieved...')
        num_tweets = 0
        for tweet in tweets:
            num_tweets += 1
            assert tweet.message() == 'message' + str(num_tweets), tweet.message()
        assert num_tweets == 2, num_tweets
