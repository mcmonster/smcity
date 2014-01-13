''' Model of the Tweets NoSQL table as well as variety of helper functions. '''

import re

from boto.dynamodb2.fields import AllIndex, HashKey, RangeKey
from boto.dynamodb2.table import Table

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

    def lat(self):
        return self.record['lat']

    def lon(self):
        return self.record['lon']

    def message(self):
        return self.record['message']

    def user(self):
        return self.record['user']

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
            HashKey('user'), RangeKey('timestamp')
        ])

    def create_tweet(self, user, message, city, timestamp, lat, lon):
        ''' 
        Creates a new Tweet using the provided data.        

        @param user User who tweeted the message
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
        assert user is not None, "User must not be None!"
        assert message is not None, "Message must not be None!"
        assert city is not None, "City must not be None!"
        assert timestamp is not None, "Timestamp must not be None!"
        assert lat is not None, "Lat must not be None!"
        assert lon is not None, "Lon must not be None!"
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
        
        # Create the database record
        data={
            'user' : user,
            'city' : city,
            'lat' : lat,
            'lat_copy' : lat,
            'lon' : lon,
            'lon_copy' : lon,
            'message' : message,
            'timestamp' : timestamp
        }
        result = self.table.put_item(data=data)

        # If we failed to create the database record
        if result is False:
            message = "Failed to create the Tweet(" + str(data) + ")!"
            logger.error(message)
            raise Exception(message)

    def get_tweets(self, city, age_limit=None, coordinate_box=None):
        '''
        Retrieves an iterator set to iterate over all tweets associated with provided city.

        @param city City whose tweets should be fetched.
        @paramType string
        @param coordinate_box Restricts to tweets within the specified coordinate box
        @paramType dictionary featuring the following keys: min_lon, min_lat, max_lon, max_lat
        @returns Iterator over the fetched data
        @returnType boto.dynamodb2.ResultSet
        '''
        assert city is not None, "city must not be None!"

        if coordinate_box is not None: # Unroll the coordinate box
            assert 'min_lon' in coordinate_box.keys(), "Expected min_lon as key in coordinate_box"
            assert 'min_lat' in coordinate_box.keys(), "Expected min_lat as key in coordinate box"
            assert 'max_lon' in coordinate_box.keys(), "Expected max_lon as key in coordinate box"
            assert 'max_lat' in coordinate_box.keys(), "Expected max_lat as key in coordinate box"

        if age_limit is None and coordinate_box is None: # If no coordinate box or age limit is specified
            logger.debug("Scanning for records in city '%s'", city)
            return TweetIterator(self.table.scan(city__eq=city))
        elif age_limit is None: # If a coordinate box is specified
            logger.debug("Scanning for records in city '%s' inside coordinate box '%s'",
                city, coordinate_box)
            return TweetIterator(
                self.table.scan(
                    city__eq=city,
                    lat__lte=coordinate_box['max_lat'],
                    lat_copy__gte=coordinate_box['min_lat'],
                    lon__lte=coordinate_box['max_lon'],
                    lon_copy__gte=coordinate_box['min_lon']
                )
            )
        elif coordinate_box is None: # If the age limit is specified
            logger.debug("Scanning for records in city '%s' which are newer than %s",
                city, age_limit)
            return TweetIterator(
                self.table.scan(
                    city__eq=city,
                    timestamp__gte=age_limit
                )
            )
        else: # If both the age limit and coordinate box are specified
            logger.debug("Scanning for records in city '%s' which are newer than %s and inside coordinate box '%s'",
                city, age_limit, coordinate_box)
            return TweetIterator(
                self.table.scan(
                    city__eq=city,
                    lat__lte=coordinate_box['max_lat'],
                    lat_copy__gte=coordinate_box['min_lat'],
                    lon__lte=coordinate_box['max_lon'],
                    lon_copy__gte=coordinate_box['min_lon'],
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

