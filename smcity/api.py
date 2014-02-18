''' API for performing analytics on the Twitter stream. '''

import boto
import math
import numpy
import uuid

from smcity.models.maps import FutureMap

class Analytics():
    ''' Interface for performing analytics on the Twitter stream. '''

    def __init__(self, job_factory, task_queue):
        '''
        Constructor.

        @param job_factory Interface for setting up new jobs
        @paramType JobFactory
        @param task_queue Interface for posting work requests.
        @paramType TaskQueue
        @returns n/a
        '''
        assert job_factory is not None
        assert task_queue is not None

        self.job_factory = job_factory
        self.task_queue = task_queue

    def count_tweets(self, coordinate_box, resolution, on_completion, on_error):
        '''
        Counts the number of tweets that have occurred.

        @param coordinate_box Coordinates of the area in which to count tweets
        @paramType dictionary with keys {'min_lat', 'min_lon', 'max_lat', 'max_lon'}
        @param resolution Size of the grid cells in degrees lat/lon
        @paramType float
        @param on_completion Call-back function to trigger when the job has successfully completed
        @paramType function which accepts FutureMap as an argument
        @param on_error Call-back fucntion to trigger when the job fails to complete
        @paramType function which accepts FutureMap as an argument
        @returns FutureMap
        '''
        assert coordinate_box is not None
        assert resolution is not None
        assert on_completion is not None
        assert on_error is not None

        job_id = self.job_factory.create_job() # Initialize a new job for this request

        coordinate_boxes = self._generate_grid_cells(coordinate_box, resolution) # Split up the box

        self.task_queue.request_count_tweets(job_id, coordinate_boxes) # Post the work request

        # Return an asych result object that handles tracking the progress of the job
        return FutureMap(
            job_id,
            on_completion,
            on_error
        )

    def _generate_grid_cells(self, coordinate_box, resolution):
        '''
        Generates the coordinate boxes of the grid cells inside the provided area.

        @param coordinate_box Coordinates of the area in which to count tweets
        @paramType dictionary with keys {'min_lat', 'min_lon', 'max_lat', 'max_lon'}
        @param resolution Size of the grid cells in degress lat/lon
        @paramType float
        @returns list of grid coordinates
        @returnType list of dictionary
        '''
        min_lat = coordinate_box['min_lat'] # Extract the cell grid properties
        min_lon = coordinate_box['min_lon']
        max_lat = coordinate_box['max_lat']
        max_lon = coordinate_box['max_lon']
        num_rows = int(math.ceil((max_lat - min_lat) / resolution))
        num_cols = int(math.ceil((max_lon - min_lon) / resolution))
        grid_cells = []

        for row_iter in range(num_rows): # Step through the grid rows
            start_lat = min_lat + row_iter * resolution
            stop_lat = start_lat + resolution
            if row_iter + 1 == num_rows: # If this is the last row
                stop_lat = max_lat # This accounts for edge cells that are not full size

            for col_iter in range(num_cols): # Step through the grid columns
                start_lon = min_lon + col_iter * resolution
                stop_lon = start_lon + resolution
                if col_iter + 1 == num_cols: # If this is the last cell in the column
                    stop_lon = max_lon # This accounts for edge cells that are not full size

                # Write out the cell's coordinate box
                grid_cells.append({
                    'min_lat' : start_lat,
                    'min_lon' : start_lon,
                    'max_lat' : stop_lat,
                    'max_lon' : stop_lon
                })

        return grid_cells


