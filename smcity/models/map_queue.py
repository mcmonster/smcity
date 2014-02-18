''' Interface definition for the mapped task queue. '''

class MapQueue:
    ''' Interface for requesting the execution of tasks by the computing nodes '''

    def finish_task(self, task):
        '''
        Removes the provided task from the queue, preventing any other compute nodes from trying
        to handle it.

        @param task Task to be removed. Requires keys 'job_id', 'task', 'coordinate_box' to
        be unaltered since retrieving the task via get_task()
        @paramType dictionary
        @returns n/a
        '''
        raise NotImplementedError()

    def get_task(self):
        '''
        Retrieves the next task in the queue.
 
        @returns dictionary with at least keys 'job_id', 'task', 'coordinate_box' or 
        None if no task is available
        @returnType dictionary
        '''
        raise NotImplementedError()

    def request_count_tweets(self, job_id, polygon_strategy):
        '''
        Submits the requests needed to count the number of tweets in the polygon described
        by polygon_stategy.

        @param job_id Tracking id of the job
        @paramType uuid/string
        @param polygon_strategy Generates the coordinate boxes inscribing a complex polygon
        @paramType PolygonStrategy
        @returns n/a
        '''
        raise NotImplementedError()
