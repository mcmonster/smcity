'''  Unit tests for the jobs model and factory '''

import json

from boto.dynamodb2.table import Table
from ConfigParser import ConfigParser

from smcity.models.aws.aws_job import AwsJobFactory

class MockPolygonStrategy:
    def __init__(self, rep):
        self.rep = rep

    def to_dict(self):
        return self.rep

class MockPolygonStrategyFactory:
    def from_dict(self, state):
        return state['class']

class TestAwsJob:
    ''' Unit tests for the AwsJob model and factory. '''

    def setup(self):
        ''' Set up before each test '''
        # Set up the Jobs table
        self.jobs = Table('test_jobs')
        
        # Set up the AwsJobFactory instance
        self.config = ConfigParser()
        self.config.add_section('compute_api')
        self.config.set('compute_api', 'jobs_table', 'test_jobs')
        self.job_factory = AwsJobFactory(self.config, MockPolygonStrategyFactory())

        # Empty the content of the jobs table
        for job in self.jobs.scan():
            job.delete()

    def test_create_job(self):
        ''' Tests the create_job function. '''
        task = 'task'
        polygon_strategy = MockPolygonStrategy({'class' : 'mock_polygon_strategy'})

        job_id = self.job_factory.create_job(task, polygon_strategy)

        record = self.jobs.get_item(id=job_id)
        assert record['id'] == job_id, record['id']
        assert record['is_finished'] == False, record['is_finished']
        assert json.loads(record['polygon_strategy'])['class'] == 'mock_polygon_strategy', \
            json.loads(record['polygon_strategy'])['class']
        assert record['results'] == '[]', record['results']
        assert record['run_times'] == '{}', record['run_times']
        assert record['task'] == 'task', record['task']

    def test_get_job(self):
        ''' Tests the get_job function '''
        task = 'task'
        polygon_strategy = MockPolygonStrategy({'class' : 'mock_polygon_strategy'})

        job_id = self.job_factory.create_job(task, polygon_strategy)
        job = self.job_factory.get_job(job_id)

        assert job.get_is_finished() == False
        assert job.get_polygon_strategy() == 'mock_polygon_strategy', job.get_polygon_strategy()
        assert json.loads(job.get_results()) == [], json.loads(job.get_results())
        assert json.loads(job.get_run_times()) == {}, job.get_run_times()
        assert job.get_task() == 'task', job.get_task()
        
    def test_save_job(self):
        ''' Tests the save_changes function '''
        task = 'task'
        polygon_strategy = MockPolygonStrategy({'class' : 'mock_polygon_strategy'})
        coordinate_box = {'min_lat' : 0, 'min_lon' : 0, 'max_lat' : 1, 'max_lon' : 1}
        result = 5
        results_size = 10
        subtask = 'subtask'
        run_time = 999.9

        job_id = self.job_factory.create_job(task, polygon_strategy)
        job = self.job_factory.get_job(job_id)
        job.add_result(coordinate_box, result)
        job.add_run_time(subtask, run_time)
        job.set_results_size(results_size)
        job.set_is_finished(True)
        job.save_changes()

        record = self.jobs.get_item(id=job.get_id())
        assert json.loads(record['results'])[0]['min_lat'] == 0
        assert json.loads(record['results'])[0]['min_lon'] == 0
        assert json.loads(record['results'])[0]['max_lat'] == 1
        assert json.loads(record['results'])[0]['max_lon'] == 1
        assert json.loads(record['results'])[0]['result'] == result
        assert json.loads(record['run_times'])[subtask] == run_time
        assert record['results_size'] == results_size
        assert record['is_finished'] == True
        
