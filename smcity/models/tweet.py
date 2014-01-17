''' Model of the Tweets NoSQL table as well as variety of helper functions. '''

import datetime
import time
import re

from boto.dynamodb2.fields import AllIndex, HashKey, RangeKey
from boto.dynamodb2.table import Table
from threading import Thread

from smcity.logging.logger import Logger

logger = Logger(__name__)

class Tweet():
    ''' Model of the Tweets NoSQL table. '''

    def __init__(self, record):
        '''
        Constructor.

        @param record Database record corresponding to this Tweet.
        @param ~dictionary
        @returns n/a
        '''
        assert record is not None, 'record must not be None!'

        self.record = record

    def city(self):
        return self.record['city']

    def id(self):
        return self.record['id']

    def lat(self):
        return float(self.record['lat']) / 10000000

    def lon(self):
        return float(self.record['lon']) / 10000000

    def message(self):
        return self.record['message']

    def timestamp(self):
        return self.record['timestamp']
        
class TweetFactory:
    ''' Factory pattern for creating Tweet objects and their corresponding database records. '''

    def __init__(self, config):
        '''
        Constructor.

        @param config Configuration settings. Expected definitions:

        Section:     database
        Key:         tweets_table
        Type:        string
        Description: Name of the Tweets model table
        @paramType ConfigParser
        @returns n/a
        '''
        self.table = Table(config.get('database', 'tweets_table'), schema=[
            HashKey('id'), RangeKey('timestamp')
        ])

    def create_tweet(self, id, message, place, timestamp, lat, lon):
        ''' 
        Creates a new Tweet using the provided data.        

        @param id Twitter generated id of the message
        @paramType string
        @param message Tweeted message
        @paramType string
        @param city City in which the tweet was made
        @paramType string
        @param timestamp When the tweet was made. Format: YYYY-MM-dd HH24:mm:ss
        @paramType string
        @param lat Latitude at which the tweet was made
        @paramType float
        @param lon Longitude at which the tweet was made
        @paramType float
        @returns n/a
        '''
        assert id is not None, "id must not be None!"
        assert message is not None, "message must not be None!"
        assert place is not None, "place must not be None!"
        assert timestamp is not None, "timestamp must not be None!"
        assert lat is not None, "lat must not be None!"
        assert lon is not None, "lon must not be None!"
        assert (-90 <= lat) and (lat <= 90), "Expected -90 <= lat <= 90 " + \
            "but got %r" % lat
        assert (-180 <= lon) and (lon <= 180), "Expected -180 <= lon <= 180" \
            + " but got %r" % lon

        # Parse the timestamp to ensure it is properly formatted
        tokens = re.findall(r"[\w']+", timestamp)
        assert len(tokens) == 6, "Expected six tokens in timestamp, got %r" % tokens
        assert len(tokens[0]) == 4, "Expected year to be first token in YYYY format, got %r" % tokens[0]
        assert len(tokens[1]) == 2, "Expected month to be second token in MM format, got %r" % tokens[1]
        assert int(tokens[1]) > 0 and int(tokens[1]) <= 12, "Expected valid month, got %r" % tokens[1]
        assert len(tokens[2]) == 2, "Expected day to be third token in dd format, got %r" % tokens[2]
        assert int(tokens[2]) > 0 and int(tokens[2]) <= 31, "Expected valid day, got %r" % tokens[2]
        assert len(tokens[3]) == 2, "Expected hour to be fourth token in HH24 format, got %r" % tokens[3]
        assert int(tokens[3]) >= 0 and int(tokens[3]) < 24, "Expected valid hour, got %r" % tokens[3]
        assert len(tokens[4]) == 2, "Expected minute to be fifth token in mm format, got %r" % tokens[4]
        assert int(tokens[4]) >= 0 and int(tokens[4]) < 60, "Expected valid minutes, got %r" % tokens[4]
        assert len(tokens[5]) == 2, "Expected second to be sixth token in ss format, got %r" % tokens[5]
        assert int(tokens[5]) >= 0 and int(tokens[5]) < 60, "Expected valid seconds, got %r" % tokens[5]
       
        # Normalize the lat/lon values
        lat_norm = int(lat * 10000000)
        lon_norm = int(lon * 10000000)
        
        # Create the database record
        data={
            'id' : id,
            'lat' : lat_norm,
            'lat_copy' : lat_norm,
            'lon' : lon_norm,
            'lon_copy' : lon_norm,
            'message' : message,
            'place' : place,
            'timestamp' : timestamp
        }
        result = self.table.put_item(data=data)

        # If we failed to create the database record
        if result is False:
            message = "Failed to create the Tweet(" + str(data) + ")!"
            logger.error(message)
            raise Exception(message)

    def get_tweets(self, age_limit=None, coordinate_box=None):
        '''
        Retrieves an iterator set to iterate over all tweets associated with provided city.

        @param coordinate_box Restricts to tweets within the specified coordinate box
        @paramType dictionary featuring the following keys: min_lon, min_lat, max_lon, max_lat
        @returns Iterator over the fetched data
        @returnType boto.dynamodb2.ResultSet
        '''
        if coordinate_box is not None: # Unroll the coordinate box
            assert 'min_lon' in coordinate_box.keys(), "Expected min_lon as key in coordinate_box"
            assert 'min_lat' in coordinate_box.keys(), "Expected min_lat as key in coordinate box"
            assert 'max_lon' in coordinate_box.keys(), "Expected max_lon as key in coordinate box"
            assert 'max_lat' in coordinate_box.keys(), "Expected max_lat as key in coordinate box"

        if age_limit is None and coordinate_box is None: # If no coordinate box or age limit is specified
            logger.debug("Scanning for all record...")
            return TweetIterator(self.table.scan())
        elif age_limit is None: # If a coordinate box is specified
            logger.debug("Scanning for records inside coordinate box '%s'", coordinate_box)
            return TweetIterator(
                self.table.scan(
                    lat__lte=int(coordinate_box['max_lat'] * 10000000),
                    lat_copy__gte=int(coordinate_box['min_lat'] * 10000000),
                    lon__lte=int(coordinate_box['max_lon'] * 10000000),
                    lon_copy__gte=int(coordinate_box['min_lon'] * 10000000)
                )
            )
        elif coordinate_box is None: # If the age limit is specified
            logger.debug("Scanning for records which are newer than %s", age_limit)
            return TweetIterator(
                self.table.scan(timestamp__gte=age_limit)
            )
        else: # If both the age limit and coordinate box are specified
            logger.debug("Scanning for records which are newer than %s and inside coordinate box '%s'",
                age_limit, coordinate_box)
            return TweetIterator(
                self.table.scan(
                    lat__lte=int(coordinate_box['max_lat'] * 10000000),
                    lat_copy__gte=int(coordinate_box['min_lat'] * 10000000),
                    lon__lte=int(coordinate_box['max_lon'] * 10000000),
                    lon_copy__gte=int(coordinate_box['min_lon'] * 10000000),
                    timestamp__gte=age_limit
                )
            )

class TweetIterator:
    ''' Wrapper around the DynamoDB2 ResultSet iterator. '''

    def __init__(self, result_set):
        '''
        Constructor.

        @param result_set DynamoDB2 ResultSet to wrap.
        @param boto.dynamodb2.ResultSet
        @returns n/a
        '''
        assert result_set is not None, "result_set must not be None!"
 
        self.result_set = result_set

    def __iter__(self):
        return self

    def next(self):
        return Tweet(self.result_set.next())

class TweetJanitor:
    ''' Cleans up out of data tweets. '''
    
    def __init__(self, config):
        '''
        @param config Configuration settings. Expected definitions:
        Section:     database
        Key:         max_tweet_age
        Type:        int
        Description: Max time a tweet is kept in hours

        Section:     database
        Key:         tweets_table
        Type:        string
        Description: Name of the Tweets model table
        @paramType ConfigParser
        @returns n/a
        '''
        self.is_shutting_down = False
        self.max_age = config.getint('database', 'max_tweet_age')
        self.table = Table(config.get('database', 'tweets_table'), schema=[
            HashKey('id'), RangeKey('timestamp')
        ])

    def _maintain_tweets(self):
        '''
        Periodically deletes any tweets that are too old.

        @returns n/a
        '''
        last_scan = 0
        one_hour = 3600

        while not self.is_shutting_down: # While the janitor hasn't begun the shutdown process
            if time.time() - last_scan > one_hour: # If it's been over an hour since the last scan
                age_limit = datetime.datetime.fromtimestamp(time.time() - self.max_age * one_hour) \
                                             .strftime('%Y-%m-%d %H:%M:%S')
                logger.info("Scanning with an age threshold of '%s'...", age_limit)

                num_tweets_deleted = 0
                for tweet in self.table.scan(timestamp__lt = age_limit): # Fetch all of the old tweets
                    tweet.delete() # Delete the tweet
                    num_tweets_deleted += 1
                logger.info("Deleted %s old tweets!", num_tweets_deleted)

                age_limit = time.time()

            time.sleep(5) # Wait a bit before checking again 

    def maintain_tweets(self):
        '''
        Spins up a thread which periodically deletes any tweets that are too old.

        @returns n/a
        '''
        thread = Thread(target=self._maintain_tweets)
        thread.daemon = True
        thread.start() 

    def shutdown(self):
        ''' Shutdowns the maintenance thread. '''
        self.is_shutting_down = True
