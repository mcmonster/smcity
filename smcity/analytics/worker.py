''' Contains the backend worker that actually handles performing the analytical tasks. '''

from smcity.logging.logger import Logger

logger = Logger(__name__)

class Worker():
    ''' Handles actually performing the analytical tasks. '''
  
    def __init__(self, result_queue, task_queue, tweet_factory):
        '''
        Constructor.
 
        @param result_queue Interface for posting work results
        @paramType ResultQueue
        @param task_queue Interface for retrieving tasks to be performed
        @paramType TaskQueue
        @param tweet_factory Interface for retrieving tweets
        @paramType TweetFactory
        @returns n/a
        '''
        assert result_queue is not None
        assert task_queue is not None
        assert tweet_factory is not None

        self.is_shutting_down = False
        self.result_queue = result_queue
        self.task_queue = task_queue
        self.tweet_factory = tweet_factory

    def _count_tweets(self, job_id, coordinate_box):
        '''
        Counts the number of tweets that have occurred within the specified coordinate box.

        @param job_id Tracking id of the job
        @paramType uuid/string
        @param coordinate_box Area in which to search
        @paramType uuid/string
        @returns n/a
        '''
        assert job_id is not None

        num_tweets = 0 # Count the number of tweets in the specified area
        for tweet in self.tweet_factory.get_tweets(coordinate_box=coordinate_box):
            num_tweets += 1

        self.result_queue.post_count_tweets_result(job_id, coordinate_box, num_tweets)

    def perform_tasks(self):
        '''
        Consumes tasks from the task queue and performs the work requested.
  
        @returns n/a
        '''
        while not self.is_shutting_down:
            try:
                task = self.task_queue.get_task() # Get the next task
                if task is None:
                    continue

                logger.debug("%s Received request for '%s'...", task['job_id'], task['task'])
                if task['task'] == 'count_tweets':
                    self._count_tweets(task['job_id'], task['coordinate_box'])
                else:
                    raise Exception("%s Unknown task '%s'!", task['job_id'], task['task'])

                self.task_queue.finish_task(task) # Finish the task
            except:
                logger.exception()

    def shutdown(self):
        ''' Shutdowns down the worker perform_task routine. '''
        self.is_shutting_down = True
