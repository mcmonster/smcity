''' Public facing API for requesting analytic calculations on geographic areas. '''

from ConfigParser import ConfigParser

from smcity.analytics.asynch_result import AsynchResultFactory
from smcity.models.aws.aws_job import AwsJobFactory
from smcity.models.aws.aws_map_queue import AwsMapQueue
from smcity.polygons.abstract_polygon_strategy_factory import AbstractPolygonStrategyFactory
from smcity.styles.abstract_style_strategy_factory import AbstractStyleStrategyFactory
from smcity.styles.color_swatch import ColorSwatchFactory

class AwsApi:
    ''' Implementation of the public facing API for requesting analytic calculations. '''

    def __init__(self, config_file):
        '''
        Constructor.
 
        @param config_file Config file to use with the api
        @paramType string
        @returns n/a
        '''
        # Load the configuration settings
        config = ConfigParser()
        configFile = open(config_file)
        config.readfp(configFile)
        configFile.close()

        # Set up the required components
        color_swatch_factory = ColorSwatchFactory()
        style_strategy_factory = AbstractStyleStrategyFactory(color_swatch_factory)
        polygon_strategy_factory = AbstractPolygonStrategyFactory(style_strategy_factory)
        job_factory = AwsJobFactory(config, polygon_strategy_factory)
        self.map_queue = AwsMapQueue(config, job_factory)
        self.result_factory = AsynchResultFactory(job_factory)

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
