''' Model for the request queue used to communicate work requests from the end-user api to the backend '''

import boto.sqs
import json

from boto.sqs.message import Message

from smcity.logging.logger import Logger
from smcity.models.request_queue import RequestQueue

logger = Logger(__name__)

class AwsRequestQueue(RequestQueue):
    ''' AWS specific implementation of the request queue. '''

    def __init__(self, config, strategy_factory):
        '''
        Constructor.

        @param config Configuration settings fro the queue. Requires the following definitions:

        Section: compute_api
        Key:     region
        Type:    string
        Desc:    AWS data center to connect to
 
        Section: compute_api
        Key:     request_queue
        Type:    string
        Desc:    Name of the request queue in SQS
        @paramType ConfigParser
        @param strategy_factory PolygonStategy factory that can marshal polygon strategies from dicts
        @paramType PolygonStrategyFactory
        @returns n/a
        '''
        assert strategy_factory is not None
        
        self.strategy_factory = strategy_factory

        # Retrieve the request queue
        conn = boto.sqs.connect_to_region(config.get('compute_api', 'region'))
        self.queue = conn.get_queue(config.get('compute_api', 'request_queue'))
        assert self.queue is not None, \
            "Request queue '%s' does not exist!" % config.get('compute_api', 'request_queue')

        # Set up a dictionary for tracking currently consumed messages
        self.in_progress_messages = {}

    def finish_request(self, request):
        ''' {@inheritDocs} '''
        request_hash = self._generate_hash(request)

        if request_hash in self.in_progress_messages.keys(): # If the corresponding message exists
            self.queue.delete_message(self.in_progress_messages[request_hash])
            del self.in_progress_messages[request_hash]
        else: # If there is not corresponding message
            raise Exception("Corresponding message does not exist for request(%s)" % str(request))

    def _generate_hash(self, request):
        ''' {@inheritDocs} '''
        return request['job_id'] + '_' + request['task']

    def get_request(self):
        ''' {@inheritDocs} '''
        message = self.queue.read()

        if message is None: # If no message is available
            return None

        request = json.loads(message.get_body()) # Parse the request
        
        # Verify that the message is well formed
        if ('job_id' not in request.keys() or 
            'task' not in request.keys() or
            'polygon_strategy' not in request.keys()):
            logger.warn('Malformed request: %s', str(request))
            return None # No need to delete the message, let it drop into the dead letter queue

        request_hash = self._generate_hash(request) # Save the message for later deletion
        self.in_progress_messages[request_hash] = message

        # Marshall the polygon strategy
        request['polygon_strategy'] = self.strategy_factory.from_dict(request['polygon_strategy'])
        
        return request

    def request_count_tweets(self, job_id, polygon_strategy):
        ''' {@inheritDocs} '''
        assert job_id is not None
        
        message = Message() # Set up the message
        message.set_body(json.dumps({
            'job_id' : job_id,
            'task' : 'count_tweets',
            'polygon_strategy' : polygon_strategy.to_dict()
        }))

        result = self.queue.write(message) # Write out the request
        assert result is not None, 'Failed to push request to queue!'
        
