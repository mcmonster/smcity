''' Unit tests for the Analytics api. '''

from smcity.api import Analytics

class MockJobFactory():
    def create_job(self):
        return 'job_id'

class MockTaskQueue():
    def request_count_tweets(self, job_id, coordinate_boxes):
        self.request = {'job_id' : job_id, 'task' : 'count_tweets', 'coordinate_boxes' : coordinate_boxes}

class TestAnalytics():
    ''' Unit tests for the Analytics class. '''

    def _on_complete(self, future_map):
        print "_on_complete"

    def _on_error(self, future_map):
        print "_on_error"

    def setup(self):
        ''' Set up before each test. '''
        # Set up the Analytics instance and its dependencies
        self.job_factory = MockJobFactory()
        self.task_queue = MockTaskQueue()
        self.analytics = Analytics(self.job_factory, self.task_queue)

    def test_count_tweets(self):
        ''' Tests the count_tweets function. '''
        self.analytics.count_tweets(
            coordinate_box={'min_lat' : 0, 'min_lon' : 0, 'max_lat' : 1, 'max_lon' : 1},
            resolution = 1,
            on_completion = self._on_complete,
            on_error = self._on_error
        )
        
        request = self.task_queue.request
        assert request is not None
        assert request['job_id'] == 'job_id', request['job_id']
        assert request['task'] == 'count_tweets', request['task']
        assert len(request['coordinate_boxes']) == 1, len(request['coordinate_boxes'])
        assert request['coordinate_boxes'][0]['min_lat'] == 0, request['coordinate_boxes'][0]['min_lat']
        assert request['coordinate_boxes'][0]['min_lon'] == 0, request['coordinate_boxes'][0]['min_lon']
        assert request['coordinate_boxes'][0]['max_lat'] == 1, request['coordinate_boxes'][0]['max_lat']
        assert request['coordinate_boxes'][0]['max_lon'] == 1, request['coordiante_boxes'][0]['max_lon']
