''' Model for the result queue used to communicate results of work requests to the computing nodes. '''

import boto.sqs
import json

from boto.sqs.message import Message

from smcity.models.reduce_queue import ReduceQueue

class AwsReduceQueue(ReduceQueue):
    ''' AWS specific implementation of the reduce queue. '''

    def __init__(self, config):
        '''
        Constructor.

        @param config Configuration settings for the queue. Requires the following definitions:

        Section: compute_api
        Key:     region
        Type:    string
        Desc:    AWS data center to connect to
 
        Section: compute_api
        Key:     reduce_queue
        Type:    string
        Desc:    name of the result queue in SQS
        @paramType ConfigParser
        @returns n/a
        '''
        # Retrieve the results queue
        conn = boto.sqs.connect_to_region(config.get('compute_api', 'region'))
        self.queue = conn.get_queue(config.get('compute_api', 'reduce_queue'))
        assert self.queue is not None, \
            "Reduce queue '%s' does not exist!" % config.get('compute_api', 'reduce_queue')

        # Set up a dictionary for tracking currently consumed messages
        self.in_progress_messages = {}

    def finish_result(self, result):
        ''' {@inheritDocs} '''
        result_hash = self._generate_hash(result)
 
        if result_hash in self.in_progress_messages.keys(): # If the corresponding message exists
            self.queue.delete_message(self.in_progress_messages[result_hash])
            del self.in_progress_messages[result_hash]
        else: # If there is not corresponding message
            raise Exception("Corresponding message does not exist for result(%s)" % str(result))

    def _generate_hash(self, result):
        '''
        Generates a hash of the result to be used as a key in the in_progress_messages dictionary.

        @param result Result to be hashed
        @paramType dictionary
        @returns Hash of task
        @returnType string
        '''
        result_hash = result['job_id'] + '_' + result['task'] + '_'
        result_hash += str(result['coordinate_box']['min_lat']) + '_'
        result_hash += str(result['coordinate_box']['max_lat']) + '_'
        result_hash += str(result['coordinate_box']['min_lon']) + '_'
        result_hash += str(result['coordinate_box']['max_lon'])

        return result_hash

    def get_result(self):
        ''' {@inheritDocs} '''
        message = self.queue.read()

        if message is None: # If no message is available
            return None

        result = json.loads(message.get_body()) # Parse the task result

        # Verify that the message is well formed
        if ('job_id' not in result.keys() or
            'task' not in result.keys() or 
            'coordinate_box' not in result.keys()):
            logger.warn('Malformed result request: %s', str(result))
            return None # No need to delete the message, let it drop into the dead letter queue
        
        result_hash = self._generate_hash(result) # Save the message for later deletion
        self.in_progress_messages[result_hash] = message

        return result

    def post_count_tweets_result(self, job_id, coordinate_box, count):
        '''
        Submits the results of the tweet count.

        @param job_id Tracking id of the job
        @paramType uuid/string
        @param coordinate_box Box in which the tweets were counted
        @paramType dictionary
        @param count # of tweets in the coordinate_box
        @paramType int
        @returns n/a
        '''
        assert job_id is not None
        assert coordinate_box is not None
        assert count is not None

        message = Message() # Set up the message
        message.set_body(json.dumps({
            'job_id' : job_id,
            'task' : 'count_tweets',
            'count' : count,
            'coordinate_box' : coordinate_box
        }))

        result = self.queue.write(message) # Write out the request
        assert result is not None, 'Failed to push results to queue!'

