''' Unit tests for the AwsMapQueue class. '''

import boto.sqs
import ConfigParser
import json

from boto.sqs.message import Message

from smcity.models.aws.aws_map_queue import AwsMapQueue

class MockPolygonStrategy:
    def __init__(self, coordinate_boxes):
        self.coordinate_boxes = coordinate_boxes

    def get_inscribed_boxes(self):
        return self.coordinate_boxes

class MockJobFactory:
    def create_job(self, task, polygon_strategy, num_sub_areas):
        self.created_job = (task, polygon_strategy, num_sub_areas)
        return 'job_id'

class TestAwsMapQueue():
    ''' Unit tests for the AwsMapQueue class. '''

    def setup(self):
        ''' Set up before each test. '''
        # Set up the instance configuration
        self.config = ConfigParser.ConfigParser()
        self.config.add_section('compute_api')
        self.config.set('compute_api', 'region', 'us-west-2')
        self.config.set('compute_api', 'map_queue', 'test_map')

        # Connecting to SQS
        conn = boto.sqs.connect_to_region('us-west-2')
        self.queue = conn.get_queue('test_map')

        # Emptying the test queue
        message = self.queue.read()
        while message is not None:
            self.queue.delete_message(message)
            message = self.queue.read()

    def test_finish_task(self):
        ''' Tests the finish_task function. '''
        map_queue = AwsMapQueue(self.config, MockJobFactory())

        message = Message()
        message.set_body(json.dumps({
            'job_id' : 'job_id',
            'task' : 'task',
            'coordinate_box' : {'min_lat' : 0, 'min_lon' : 0, 'max_lat' : 1, 'max_lon' : 1}
        }))
        self.queue.write(message)
        task = map_queue.get_task()
        assert len(map_queue.in_progress_messages.keys()) == 1

        map_queue.finish_task(task)
        assert len(map_queue.in_progress_messages.keys()) == 0

        message = self.queue.read()
        assert message is None

    def test_get_task(self):
        ''' Tests the get_task function. '''
        map_queue = AwsMapQueue(self.config, MockJobFactory())

        # Try to retrieve a task when none are available
        task = map_queue.get_task()
        assert task is None, task

        # Try to retrieve an available request
        message = Message()
        message.set_body(json.dumps({
            'job_id' : 'job_id', 
            'task' : 'task',
            'coordinate_box' : {'min_lat' : 0, 'min_lon' : 0, 'max_lat' : 1, 'max_lon' : 1}
        }))
        self.queue.write(message)
        task = map_queue.get_task()
        assert task is not None
        assert task['job_id'] == 'job_id', task['job_id']
        assert task['task'] == 'task', task['task']
        assert task['coordinate_box']['min_lat'] == 0, task['coordinate_box']['min_lat']
        assert task['coordinate_box']['max_lat'] == 1, task['coordinate_box']['max_lat']
        assert task['coordinate_box']['min_lon'] == 0, task['coordinate_box']['min_lon']
        assert task['coordinate_box']['max_lon'] == 1, task['coordinate_box']['max_lon']

    def test_request_count_tweets(self):
        ''' Tests the request_count_tweets function. '''
        coordinate_box_1 = {'min_lat' : 0, 'min_lon' : 0, 'max_lat' : 1, 'max_lon' : 1}
        job_factory = MockJobFactory()
        map_queue = AwsMapQueue(self.config, job_factory)      
  
        # Make the request
        strategy = MockPolygonStrategy([coordinate_box_1])
        map_queue.request_count_tweets(strategy)

        # Check the results
        sqs_message = self.queue.read()
        assert sqs_message is not None
        message = json.loads(sqs_message.get_body())
        assert message['job_id'] == 'job_id', message['job_id']
        assert message['task'] == 'count_tweets', message['task']
        assert message['coordinate_box']['min_lat'] == 0, message['coordinate_box']['min_lat']
        assert message['coordinate_box']['max_lat'] == 1, message['coordinate_box']['max_lat']
        assert message['coordinate_box']['min_lon'] == 0, message['coordinate_box']['min_lon']
        assert message['coordinate_box']['max_lon'] == 1, message['coordinate_box']['max_lon']
        
        sqs_message = self.queue.read()
        assert sqs_message is None

        job_task, job_strategy, job_size = job_factory.created_job
        assert job_task == 'count_tweets', job_task
        assert job_strategy == strategy, str(job_strategy)
        assert job_size == 1, job_size
