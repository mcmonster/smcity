''' AWS specific implementation of the Job model and the corresponding JobFactory. '''

import json

from boto.dynamodb2.table import Table
from uuid import uuid4

from smcity.errors import CreateError, ReadError, UpdateError
from smcity.logging.logger import Logger
from smcity.models.job import Job, JobFactory

logger = Logger(__name__)

class AwsJob(Job):
    ''' AWS specific implementation of the Job model '''

    def __init__(self, record, polygon_strategy):
        '''
        Constructor.

        @param record Database record describing the job's state
        @paramType DynamoDB record
        @param polygon_strategy Strategy used to break up this job's area of interest
        @returns n/a
        '''
        assert record is not None
        assert polygon_strategy is not None

        assert 'id' in record.keys()
        assert 'is_finished' in record.keys()
        assert 'polygon_strategy' in record.keys()
        assert 'results' in record.keys()
        assert 'run_times' in record.keys()
        assert 'task' in record.keys()
        
        self.needs_to_be_saved = False
        self.record = record
        self.polygon_strategy = polygon_strategy

    def add_result(self, coordinate_box, result):
        ''' {@inheritDocs} '''
        results = json.loads(self.record['results'])
        results.append({
            'min_lat' : coordinate_box['min_lat'],
            'min_lon' : coordinate_box['min_lon'],
            'max_lat' : coordinate_box['max_lat'],
            'max_lon' : coordinate_box['max_lon'],
            'result' : result
        })
        self.record['results'] = json.dumps(results)

        if len(results) == self.record['num_sub_areas']: # If we have received all the sub-area results
            self.record['is_finished'] = True

        self.needs_to_be_saved = True

    def add_run_time(self, subtask, run_time):
        ''' {@inheritDocs} '''
        run_times = json.loads(self.record['run_times'])

        # If this will change the record
        if subtask not in run_times.keys() or run_time != run_times[subtask]:
            run_times[subtask] = run_time
            self.record['run_times'] = json.dumps(run_times)
            self.needs_to_be_saved = True
    
    def get_id(self):
        ''' {@inheritDocs} '''
        return self.record['id']

    def get_is_finished(self):
        ''' {@inheritDocs} '''
        return self.record['is_finished']

    def get_polygon_strategy(self):
        ''' {@inheritDocs} '''
        return self.polygon_strategy

    def get_results(self):
        ''' {@inheritDocs} '''
        return self.record['results']

    def get_run_times(self):
        ''' {@inheritDocs} '''
        return self.record['run_times']

    def get_task(self):
        ''' {@inheritDocs} '''
        return self.record['task']

    def save_changes(self):
        ''' {@inheritDocs} '''
        if self.needs_to_be_saved:
            if not self.record.partial_save():
                raise UpdateError('%s Failed to update database entry!' % self.record['id'])

            self.needs_to_be_saved = True

    def set_is_finished(self, is_finished):
        ''' {@inheritDocs} '''
        if self.record['is_finished'] != is_finished:
            self.record['is_finished'] = is_finished
            self.needs_to_be_saved = True

    def set_results_size(self, size):
        ''' {@inheritDocs} '''
        if self.record['results_size'] != size:
            self.record['results_size'] = size
            self.needs_to_be_saved = True

class AwsJobFactory(JobFactory):
    ''' AWS specific implementation of the JobFactory '''

    def __init__(self, config, strategy_factory):
        '''
        Constructor.

        @param config Configuration settings. Requires the following definitions:

        Section: database
        Key:     jobs_table
        Type:    string
        Desc:    Name of the NoSQL table containing the job records
        @paramType ConfigParser
        @param strategy_factory Interface for marshalling polygon strategies
        @paramType PolygonStrategyFactory
        @returns n/a
        '''
        assert config is not None
        assert strategy_factory is not None

        self.jobs = Table(config.get('database', 'jobs_table'))
        self.strategy_factory = strategy_factory

    def create_job(self, task, polygon_strategy, num_sub_areas):
        ''' {@inheritDocs} ''' 
        assert task is not None
        assert num_sub_areas > 0, num_sub_areas

        job_id = str(uuid4())

        result = self.jobs.put_item(data={
            'id' : job_id,
            'is_finished' : False,
            'num_sub_areas' : num_sub_areas,
            'polygon_strategy' : json.dumps(polygon_strategy.to_dict()),
            'results' : '[]',
            'run_times' : '{}',
            'task' : task
        })

        if result is False:
            raise CreateError("Failed to create job(%s)!" % job_id)

        return job_id

    def get_job(self, job_id):
        ''' {@inheritDocs} '''
        record = self.jobs.get_item(id = str(job_id))
        if record is None:
            raise ReadError("Job(%s) does not exist!" % job_id)
        
        polygon_strategy = self.strategy_factory.from_dict(json.loads(record['polygon_strategy']))
        return AwsJob(record, polygon_strategy)
