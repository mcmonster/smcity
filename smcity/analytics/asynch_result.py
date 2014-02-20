''' Provides an interface for retrieving the results of analytic calculations. '''

import time

from smcity.errors import NotReadyError

class AsynchResult:
    ''' Interface for retrieving the results of analytic calculations. '''

    def __init__(self, job_factory, job_id, polygon_strategy):
        '''
        Constructor.

        @param job_factory Interface for refreshing the tracked job's state
        @paramType JobFactory
        @param job_id Unique tracking if of the job this result is tracking
        @paramType string/uuid
        @param polygon_strategy Used to construct the final result from the various sub-area results
        @paramType PolygonStrategy
        @return n/a
        '''
        assert job_factory is not None
        assert job_id is not None
        assert polygon_strategy is not None
        
        self.job_factory = job_factory
        self.job_id = job_id
        self.polygon_strategy = polygon_strategy

    def get_results_geojson(self):
        '''
        Generates GeoJSON encoded results. This function is not blocking and expects
        the job to be finished when called (@see is_finished())
 
        @returns GeoJSON encoded results
        @returnType string/GeoJSON
        @throws Exception if the job is not finished
        @throwType NotReadyError
        '''
        job = self.job_factory.get_job(self.job_id)

        if not job.is_finished(): # If the job isn't finished
            raise NotReadyError()

        results = job.get_results() # Retrieve the sub-area results
        
        return self.polygon_strategy.encode_results_geojson(results)

    def get_geojson_results_blocking(self, timeout=None):
        '''
        Blocking version of the get_results_geojson function.

        @param timeout Blocking limit in seconds, if none no blocking limit is used
        @paramType int
        @returns GeoJSON encoded results
        @returnType string/GeoJSON
        @throws Exception if the job is not finished within the blocking limit
        @throwType NotReadyError
        '''
        start_time = time.time()

        while time.time() < start_time + timeout: # Keep polling for results until blocking time runs out
            job = self.job_factory.get_job(job_id)

            if job.is_finished(): # If the job is finished
                return self.polygon_strategy.encode_results_geojson(results)
            else:
                time.sleep(1) # Wait a second to keep from pounding AWS with requests

        raise NotReadyError() # If we made it here, no results were retrieved in time
        
    def is_finished(self):
        '''
        @returns Whether or not the job is finished.
        @returnType
        '''
        job = self.job_factory.get_job(job_id)

        return job.is_finished()

class AsynchResultFactory:
    ''' Handles creating new AsynchResult object. '''

    def __init__(self, job_factory):
        '''
        Constructor.
 
        @param job_factory Interface for fetching jobs
        @paramType JobFactory
        @returns n/a
        '''
        assert job_factory is not None
        
        self.job_factory = job_factory

    def create(self, job_id, polygon_strategy):
        '''
        Creates an AsynchResult that monitors and provides access to the results of the specified job
 
        @param job_id Tracking of the job to monitor
        @paramType string/uuid
        @param polygon_strategy Used to restruct the results when the job is finished
        @paramType PolygonStrategy
        @returns AsynchResult monitoring the specified job
        @returnType AsynchResult
        '''
        return AsynchResult(job_factory, job_id, polygon_strategy)
