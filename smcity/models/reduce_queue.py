''' Interface definition for reduce queue. '''

class ReduceQueue:
    ''' Abstract interface for reduce queue that aggregates compute node results. '''

    def finish_result(self, result):
        '''
        Removes the provided result's message from the queue, preventing any other result consumers
        from trying to handle it.

        @param result Result to be removed. Requires keys 'job_id', 'task', 'coordinate_box' to 
        be unaltered since retrieving the result via get_result()
        @paramType dictionary
        @returns n/a
        '''
        raise NotImplementedError()

    def get_result(self):
        '''
        Retrieves a task result from the queue.

        @returns dictionary with at least keys 'job_id', 'task', 'coordinate_box' or None if
        no result is available
        @returnType dictionary
        '''
        raise NotImplementedError()

    def post_count_tweets_result(self, job_id, coordinate_box, count):
        '''
        Submits the results of a count tweet task.

        @param job_id Tracking id of the job
        @paramType uuid/string
        @param coordinate_box Box in which the tweets were counted
        @paramType dictionary
        @param count # of tweets in the coordinate_box
        @paramType int
        @returns n/a
        '''
        raise NotImplementedError()
