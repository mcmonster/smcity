''' Unit tests for the Worker class. '''

import time

from threading import Thread

from smcity.analytics.worker import Worker

class MockResultQueue():
    def post_count_tweets_result(self, job_id, coordinate_box, count):
        self.job_id = job_id
        self.coordinate_box = coordinate_box
        self.count = count

class MockTaskQueue():
    def finish_task(self, task):
        self.finished_task = task

    def get_task(self):
        task = self.task
        self.task = None
        return task

class MockTweetFactory():
    def get_tweets(self, age_limit=None, coordinate_box=None):
        return self.tweets

class TestWorker():
    ''' Unit tests for the Worker class. '''
 
    def setup(self):
        ''' Set up before each test. '''
        # Set up the worker and its dependencies
        self.result_queue = MockResultQueue()
        self.task_queue = MockTaskQueue()
        self.tweet_factory = MockTweetFactory()
        self.worker = Worker(self.result_queue, self.task_queue, self.tweet_factory)
        self.worker_thread = Thread(target=self.worker.perform_tasks)
        self.worker_thread.is_daemon = False

    def test_perform_tasks_count_tweets(self):
        ''' Tests the perform_tasks function when a count_tweets task is received. '''
        # Load the test data
        self.task_queue.task = {
            'job_id' : 'job_id', 
            'task' : 'count_tweets',
            'coordinate_box' : {'min_lat' : 0, 'min_lon' : 0, 'max_lat' : 0, 'max_lon' : 0}
        }
        self.tweet_factory.tweets = ['tweet', 'tweet', 'tweet']
 
        # Launch the worker thread
        self.worker_thread.start()

        # Wait a moment before shutting the thread down
        time.sleep(1)
        self.worker.shutdown()

        # Check the results
        assert self.result_queue.job_id == 'job_id', self.result_queue.job_id
        assert self.result_queue.coordinate_box == {
            'min_lat' : 0, 'min_lon' : 0, 'max_lat' : 0, 'max_lon' : 0
        }, self.result_queue.coordinate_box
        assert self.result_queue.count == 3, self.result_queue.count
        
        assert self.task_queue.finished_task is not None
