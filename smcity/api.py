''' Public facing API for requesting analytic calculations on geographic areas. '''

from smcity.models.aws.aws_map_queue import AwsMapQueue
from smcity.analytics.asynch_result import AsynchResultFactory

class Api:
    ''' Implementation of the public facing API for requesting analytic calculations. '''

    def __init__(self, result_factory = AsynchResultFactory, map_queue=AwsMapQueue):
        '''
        Constructor.
 
        @param result_factory Interface for creating new asynchresult objects
        @paramType AsynchResultFactory
        @param map_queue Interface for sending sub area requests to the compute nodes
        @paramType MapQueue
        @returns n/a
        '''
        assert result_factory is not None
        assert map_queue is not None

        self.result_factory = result_factory
        self.map_queue = map_queue

    def count_tweets(self, polygon_strategy):
        '''
        Counts tweets in the area described by the provided polygon strategy.
 
        @param polygon_strategy Describes the areas whose tweets are to be counted
        @paramType PolygonStrategy
        @return Interface for checking the progress of the calculation and retrieving the results
        @returnType AsynchResult
        '''
        assert polygon_strategy is not None

        job_id = self.map_queue.request_count_tweets(polygon_strategy)
        
        return self.result_factory.create(job_id, polygon_strategy)
