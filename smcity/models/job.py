''' Interface definition for the database record representing a job. '''

class Job:
    ''' Models a compute job and its underlying database record. '''

    def add_result(self, coordinate_box, result):
        '''
        Adds to the accumulating results for the area of interest.
    
        @param coordinate_box Area described by the results
        @paramType dictionary with keys 'min_lat', 'min_lon', 'max_lat', 'max_lon'
        @param results Result for the area
        @paramType typically float, but anything that handles str()
        @returns n/a
        '''
        raise NotImplementedError()

    def add_run_time(self, subtask, run_time):
        '''
        Records the run time of a sub-task associated with the job.
 
        @param subtask Sub-task whose run time is being reported
        @paramType string
        @param run_time Run time in seconds
        @paramType float
        @returns n/a
        '''
        raise NotImplementedError()

    def get_id(self):
        '''
        @returns Id of the job
        @returnType uuid/string
        '''
        raise NotImplementedError()

    def get_is_finished(self):
        '''
        @returns Whether or not the job is finished
        @returnType uuid/string
        '''
        raise NotImplementedError()

    def get_polygon_strategy(self):
        '''
        @returns The polygon strategy used to break down the job's area of interest
        @returnType PolygonStrategy
        '''
        raise NotImplementedError()

    def get_results(self):
        '''
        @returns Results so far for the job's area of interest
        @returnType list of dictionaries containing keys 'min_lat', 'min_lon', 'max_lat', 'max_lon', 'result'
        '''
        raise NotImplementedError()

    def get_run_times(self):
        '''
        @returns Run time of various operations associated with the job
        @returnType dictionary of task, run time (secs) pairs
        '''
        raise NotImplementedError()

    def get_task(self):
        '''
        @returns Analytics task being performed on the jobs' area of interest
        @returnType string
        '''
        raise NotImplementedError()

    def results_size(self):
        '''
        @returns Expected final size of the results
        @returnType int
        '''
        raise NotImplementedError()

    def save_changes(self):
        '''
        Saves changes to the underlying database record.

        @returns n/a
        @throws If unable to update
        @throwType UpdateError
        '''
        raise NotImplementedError()

    def set_is_finished(self, is_finished):
        '''
        @param is_finished Whether or not the job is finished
        @paramType boolean
        @returns n/a
        '''
        raise NotImplementedError()

    def set_results_size(self, size):
        '''
        @param size Size of the expected result set
        @paramType int
        @returns n/a
        '''
        raise NotImplementedError()

class JobFactory:
    ''' Constructs and fetches job instances. '''

    def create_job(self, task, polygon_strategy):
        '''
        Creates a new job instance.
 
        @param task Task to be performed
        @paramType string
        @param polygon_strategy Strategy used to break down the job's area of interest
        @paramType PolygonStrategy
        @returns Tracking id of the job
        @returnType string/uuid
        '''
        raise NotImplementedError()

    def get_job(self, job_id):
        '''
        Retrieves the specified job.

        @param job_id Id of the job
        @paramType string/uuid
        @returns Associated job 
        @returnType Job
        '''
        raise NotImplementedError()
