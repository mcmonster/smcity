''' Model for the task queue used to communicate work requests to the computing nodes. '''

import boto.sqs
import json

from boto.sqs.message import Message

from smcity.logging.logger import Logger
from smcity.models.map_queue import MapQueue

logger = Logger(__name__)

class AwsMapQueue(MapQueue):
    ''' AWS specific implementation of the map queue. '''

    def __init__(self, config, job_factory):
        '''
        Constructor.
 
        @param config Configuration settings for the queue. Requires the following
        definitions:
 
        Section: compute_api
        Key:     region
        Type:    string
        Desc:    AWS data center to connect to

        Section: compute_api
        Key:     map_queue
        Type:    string
        Desc:    name of the map queue in SQS
        @paramType ConfigParser
        @param job_factory Interface for retrieving job records
        @paramType JobFactory
        @returns n/a
        '''
        assert job_factory is not None

        self.job_factory = job_factory

        # Retrieve the map queue
        conn = boto.sqs.connect_to_region(config.get('compute_api', 'region'))
        self.queue = conn.get_queue(config.get('compute_api', 'map_queue'))
        assert self.queue is not None, \
            "Map queue '%s' does not exist!" % config.get('compute_api', 'map_queue')

        # Set up a dictionary for tracking currently consumed messages
        self.in_progress_messages = {}

    def finish_task(self, task):
        ''' {@inheritDocs} '''
        task_hash = self._generate_hash(task)

        if task_hash in self.in_progress_messages.keys(): # If the corresponding message exists
            self.queue.delete_message(self.in_progress_messages[task_hash])
            del self.in_progress_messages[task_hash]
        else: # If there is not corresponding message
            raise Exception("Corresponding message does not exist for task(%s)" % str(task))

    def _generate_hash(self, task):
        '''
        Generates a hash of the task to be used as a key in the in_progress_messages dictionary.

        @param task Task to be hashed
        @paramType dictionary
        @returns Hash of task
        @returnType string
        '''
        task_hash = task['job_id'] + '_' + task['task'] + '_'
        task_hash += str(task['coordinate_box']['min_lat']) + '_'
        task_hash += str(task['coordinate_box']['max_lat']) + '_'
        task_hash += str(task['coordinate_box']['min_lon']) + '_'
        task_hash += str(task['coordinate_box']['max_lon'])
        
        return task_hash

    def get_task(self):
        ''' {@inheritDocs} '''
        message = self.queue.read()

        if message is None: # If no message is available
            return None

        task = json.loads(message.get_body()) # Parse the task request

        # Verify that the message is well formed
        if ('job_id' not in task.keys() or
            'task' not in task.keys() or
            'coordinate_box' not in task.keys()):
            logger.warn('Malformed task request: %s', str(task))
            return None # No need to delete the message, let it drop into the dead letter queue

        task_hash = self._generate_hash(task) # Save the message for later deletion
        self.in_progress_messages[task_hash] = message
 
        return task

    def request_count_tweets(self, polygon_strategy):
        ''' {@inheritDocs} '''
        # Break the area of interest into its component lat/lon boxes
        coordinate_boxes = polygon_strategy.get_inscribed_boxes()

        # Create a new job for this request
        job_id = self.job_factory.create_job('count_tweets', polygon_strategy, len(coordinate_boxes))

        for coordinate_box in coordinate_boxes: # Write out each of the coordinate boxes
            message = Message() # Set up the message
            message.set_body(json.dumps({
                'job_id' : job_id,
                'task' : 'count_tweets',
                'coordinate_box' : coordinate_box,
            }))
            
            result = self.queue.write(message) # Write out the request
            assert result is not None, 'Failed to push request to queue!'

        return job_id
