''' Models for mapped results. '''

class FutureMap:
    ''' Asych map that tracks the progress of a map generating job. '''

    def __init__(self, job_id, on_completion, on_error):
        '''
        Constructor.

        @param job_id Unique id of the analytics job
        @paramType uuid/string
        @param on_completion Call-back function to call when the job is finished
        @paramType function which accepts FutureMap as an argument
        @param on_error Call-back function to call when the job fails to complete
        @paramType function which accepts FutureMap as an argument
        @returns n/a
        '''
        assert job_id is not None
        assert on_completion is not None
        assert on_error is not None

        self.is_complete = False
        self.is_shutting_down = False
        self.job_id = job_id
        self.map = None
        self.on_completion = on_completion
        self.on_error = on_error
        #TODO
