''' Unit tests for the AwsMapQueue class. '''

import boto.sqs
import ConfigParser
import json

from boto.sqs.message import Message

from smcity.models.aws.aws_request_queue import AwsRequestQueue

class MockPolygonStrategy:
    def __init__(self, rep):
        self.rep = rep

    def to_dict(self):
        return self.rep

class MockPolygonStrategyFactory:
    def from_dict(self, polygon_strategy):
        return MockPolygonStrategy(polygon_strategy)

class TestAwsRequestQueue():
    ''' Unit tests for the AwsRequestQueue class. '''

    def setup(self):
        ''' Set up before each test. '''
        # Set up the AwsMapQueue instance
        config = ConfigParser.ConfigParser()
        config.add_section('compute_api')
        config.set('compute_api', 'region', 'us-west-2')
        config.set('compute_api', 'request_queue', 'test_request')
        self.request_queue = AwsRequestQueue(config, MockPolygonStrategyFactory())

        # Connecting to SQS
        conn = boto.sqs.connect_to_region('us-west-2')
        self.queue = conn.get_queue('test_request')

        # Emptying the test queue
        message = self.queue.read()
        while message is not None:
            self.queue.delete_message(message)
            message = self.queue.read()

    def test_finish_request(self):
        ''' Tests the finish_request function. '''
        message = Message()
        message.set_body(json.dumps({
            'job_id' : 'job_id',
            'task' : 'task',
            'polygon_strategy' : {}
        }))
        self.queue.write(message)
        request = self.request_queue.get_request()
        assert len(self.request_queue.in_progress_messages.keys()) == 1

        self.request_queue.finish_request(request)
        assert len(self.request_queue.in_progress_messages.keys()) == 0

        message = self.queue.read()
        assert message is None

    def test_get_request(self):
        ''' Tests the get_request function. '''
        # Try to retrieve a request when none are available
        request = self.request_queue.get_request()
        assert request is None, str(request)

        # Try to retrieve an available request
        message = Message()
        message.set_body(json.dumps({
            'job_id' : 'job_id',
            'task' : 'task',
            'polygon_strategy' : {'class' : 'MockPolygonStrategy'}
        }))
        self.queue.write(message)
        request = self.request_queue.get_request()
        assert request is not None
        assert request['job_id'] == 'job_id', request['job_id']
        assert request['task'] == 'task', request['task']
        assert request['polygon_strategy'].rep['class'] == 'MockPolygonStrategy'

    def test_request_count_tweets(self):
        ''' Tests the request_count_tweets function. '''
        # Make the request
        self.request_queue.request_count_tweets(
            'job_id', MockPolygonStrategy({'class' : 'MockPolygonStrategy'})
        )

        # Check the results
        sqs_message = self.queue.read()
        assert sqs_message is not None
        message = json.loads(sqs_message.get_body())
        assert message['job_id'] == 'job_id', message['job_id']
        assert message['task'] == 'count_tweets', message['task']
        assert message['polygon_strategy']['class'] == 'MockPolygonStrategy'

        sqs_message = self.queue.read()
        assert sqs_message is None

