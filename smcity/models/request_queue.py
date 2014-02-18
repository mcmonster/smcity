''' Interface definition for request queue which passes analytics requests '''

class RequestQueue:
    ''' Queue that passes analytics requests from the end-user api to the backend '''
 
    def finish_request(self, request):
        '''
        Removes the provided request from the queue, preventing any other consumers from trying 
        to handle it.
 
        @param request Request to be removed. Requires key 'job_id', 'task' to be unaltered
        since retrieving the request via get_request()
        @paramType dictionary
        @returns n/a
        '''
        raise NotImplementedError()

    def get_request(self):
        '''
        Retrieves a request from the queue.

        @returns dictionary with at least keys 'job_id', 'task', 'polygon_strategy' or None
        if no result is available
        @returnType dictionary
        '''
        raise NotImplementedError()

    def request_count_tweets(self, job_id, polygon_strategy):
        '''
        Submits a request to have the tweets counted in the area described by polygon_strategy.

        @param job_id Tracking id of the job
        @paramType uuid/string
        @param polygon_strategy Generates the coordinate boxes inscribing a complex polygon
        @paramType PolygonStrategy
        @returns Interface for retrieving results of the operation
        @returnType AsyncResult
        '''
        raise NotImplementedError()
