''' Unit tests for the AsynchResult class. '''

from smcity.analytics.asynch_result import AsynchResult

class MockPolygonStrategy:
    def encode_results_geojson(self, results):
        return "GeoJSON: " + str(results)
 
class MockJob:
    def get_results(self):
        return self.results

    def is_finished(self):
        return self._is_finished

class MockJobFactory:
    def get_job(self, job_id):
        return self.job

class TestAsynchResult:
    ''' Unit tests for the AsynchResult class. '''

    def test_get_results_geojson(self):
        ''' Tests the get_results_geojson() function. '''
        job_factory = MockJobFactory()
        job_factory.job = MockJob()
        job_factory.job._is_finished = False

        # Try to retrieve results when the results aren't ready
        asynch_result = AsynchResult(job_factory, 'job_id', MockPolygonStrategy())
        try:
            asynch_result.get_results_geojson()
            assert False, "Failed to raise exception when results were not ready"
        except:
            print "Exception properly raised!"

        # Try to retrieve results that are ready
        job_factory.job._is_finished = True
        job_factory.job.results = 'Results'

        results = asynch_result.get_results_geojson()
        assert results == 'GeoJSON: Results', results
