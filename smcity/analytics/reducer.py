''' Contains the Reducer service implementation '''

from smcity.logging.logger import Logger

logger = Logger(__name__)

class Reducer:
    ''' Consumers results from the reduce queue and pushes them into NoSQL. '''
    
    def __init__(self, job_factory, reduce_queue):
        '''
        Constructor.

        @param job_factory Used to retrieve jobs whose results have posted
        @paramType JobFactory
        @param reduce_queue Used to retrieve results from the reduce queue
        @paramType ReduceQueue
        @returns n/a
        '''
        assert job_factory is not None
        assert reduce_queue is not None
   
        self.is_shutting_down = False
        self.job_factory = job_factory
        self.reduce_queue = reduce_queue

    def reduce_results(self):
        '''
        Continuously pulls results from the queue and adds the to the associated job in the database.

        @returns n/a
        '''
        while not self.is_shutting_down:
            try:
                result = self.reduce_queue.get_result()
            
                if result is None: # If there are currently no results available
                    continue

                logger.debug("Found result for job %s. Posting result..." % result['job_id'])
                job = self.job_factory.get_job(result['job_id']) # Update the jobs state
                job.add_result(result['coordinate_box'], result['result'])
                job.save_changes()

                self.reduce_queue.finish_result(result) # Remove the result message from the queue
            except:
                logger.exception()    
    
    def shutdown(self):
        '''
        Cleanly shutdowns down the reducer.
 
        @returns n/a
        '''
        self.is_shutting_down = True
