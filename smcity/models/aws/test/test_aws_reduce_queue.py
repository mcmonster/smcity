''' Unit tests for the ResultQueue class. '''

import boto.sqs
import json

from ConfigParser import ConfigParser

from boto.sqs.message import Message
from smcity.models.aws.aws_reduce_queue import AwsReduceQueue

class TestResultQueue():
    ''' Unit tests for the ResultQueue class. '''

    def setup(self):
        ''' Set up before each test. '''
        # Set up the ResultQueue instance
        config = ConfigParser()
        config.add_section('compute_api')
        config.set('compute_api', 'region', 'us-west-2')
        config.set('compute_api', 'reduce_queue', 'test_reduce')
        self.result_queue = AwsReduceQueue(config)

        # Connect to SQS
        conn = boto.sqs.connect_to_region('us-west-2')
        self.queue = conn.get_queue('test_reduce')
   
        # Empty the test queue
        message = self.queue.read()
        while message is not None:
            self.queue.delete_message(message)
            message = self.queue.read()

    def test_finish_result(self):
        ''' Tests the finish_result function. '''
        message = Message()
        message.set_body(json.dumps({
            'job_id' : 'job_id', 
            'task' : 'task',
            'coordinate_box' : {'min_lat' : 0, 'min_lon' : 0, 'max_lat' : 1, 'max_lon' : 1}
        }))
        self.queue.write(message)
        result = self.result_queue.get_result()
        assert len(self.result_queue.in_progress_messages.keys()) == 1

        self.result_queue.finish_result(result)
        assert len(self.result_queue.in_progress_messages.keys()) == 0
        
        message = self.queue.read()
        assert message is None

    def test_get_result(self):
        ''' Tests the get_result function. '''
        # Try to retrieve a result when none are available
        result = self.result_queue.get_result()
        assert result is None, result

        # Try to retrieve an available result
        message = Message()
        message.set_body(json.dumps({
            'job_id' : 'job_id', 
            'task' : 'task',
            'coordinate_box' : {'min_lat' : 0, 'min_lon' : 0, 'max_lat' : 1, 'max_lon' : 1}
        }))
        self.queue.write(message)
        result = self.result_queue.get_result()
        assert result is not None
        assert result['job_id'] == 'job_id', result['job_id']
        assert result['task'] == 'task', result['task']
        assert result['coordinate_box']['min_lat'] == 0, result['coordinate_box']['min_lat']
        assert result['coordinate_box']['max_lat'] == 1, result['coordinate_box']['max_lat']
        assert result['coordinate_box']['min_lon'] == 0, result['coordinate_box']['min_lon']
        assert result['coordinate_box']['max_lon'] == 1, result['coordinate_box']['max_lon']

    def test_post_count_tweets_result(self):
        ''' Tests the post_count_tweets_result. '''
        coordinate_box_1 = {'min_lat' : 0, 'min_lon' : 0, 'max_lat' : 1, 'max_lon' : 1}

        # Post the results
        self.result_queue.post_count_tweets_result(
            'job_id', coordinate_box_1, 1337
        )
        
        # Check the results
        sqs_message = self.queue.read()
        assert sqs_message is not None
        message = json.loads(sqs_message.get_body())
        assert message['job_id'] == 'job_id', message['job_id']
        assert message['task'] == 'count_tweets', message['task']
        assert message['result'] == 1337, message['result']
        assert message['coordinate_box']['min_lat'] == 0, message['coordinate_box']['min_lat']
        assert message['coordinate_box']['max_lat'] == 1, message['coordinate_box']['max_lat']
        assert message['coordinate_box']['min_lon'] == 0, message['coordinate_box']['min_lon']
        assert message['coordinate_box']['max_lon'] == 1, message['coordinate_box']['max_lon']

        sqs_message = self.queue.read()
        assert sqs_message is None

