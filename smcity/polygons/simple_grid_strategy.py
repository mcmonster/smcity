''' Simple arbitrary resolution grid strategy. '''

import geojson

from geojson import Feature, FeatureCollection, Polygon

from smcity.logging.logger import Logger
from smcity.polygons.polygon_strategy import PolygonStrategy, PolygonStrategyFactory

logger = Logger(__name__)

class SimpleGridStrategy(PolygonStrategy):
    ''' Simple arbitrary resolution grid strategy. '''

    def __init__(self, coordinate_box, resolution, style_strategy):
        '''
        Constructor. 
 
        @param coordinate_box Coordinate box which should be broken down into sub-boxes of size resolution
        @paramType dictionary with keys 'min_lat', 'max_lat', 'min_lon', 'max_lon'
        @param resolution resolution of the sub-boxes in degrees
        @paramType float
        @param style_strategy Strategy used to stylize the polygons
        @paramType StyleStrategy
        @returns n/a
        '''
        assert 'min_lat' in coordinate_box.keys()
        assert 'max_lat' in coordinate_box.keys()
        assert 'min_lon' in coordinate_box.keys()
        assert 'max_lon' in coordinate_box.keys()
        assert resolution > 0
        assert style_strategy is not None

        self.coordinate_box = coordinate_box
        self.resolution = resolution
        self.style_strategy = style_strategy

    def encode_results_geojson(self, results):
        '''
        Generates GeoJSON encoded results for the grid area requested.

        @param results Sub-area results to be encoded
        @paramType List of dictionaries containing keys 'min_lat', 'min_lon', 'max_lat', 'max_lon', 'result'
        @returns GeoJSON encoded results for the grid area
        @returnType string/GeoJSON
        '''
        features = []

        # Prime the style strategy
        self.style_strategy.prep_styling(results)
        
        for result in results: # Encode the results as polygons
            logger.debug(
                "(%s, %s) : (%s %s) = %s", result['min_lon'], result['min_lat'], result['max_lon'],
                result['max_lat'], result['result']
            )
            corner1 = (result['min_lon'], result['min_lat'])
            corner2 = (result['min_lon'], result['max_lat'])
            corner3 = (result['max_lon'], result['max_lat'])
            corner4 = (result['max_lon'], result['min_lat'])

            polygon = Polygon([[corner1, corner2, corner3, corner4, corner1]])
            style = self.style_strategy.style_result_geojson(result)
            features.append(Feature(geometry=polygon, properties=style))

        return geojson.dumps(FeatureCollection(features))

    def get_inscribed_boxes(self):
        ''' {@inheritDocs} '''
        coordinate_boxes = []

        max_lat = self.coordinate_box['max_lat']
        max_lon = self.coordinate_box['max_lon']
        min_lat = self.coordinate_box['min_lat']
        min_lon = self.coordinate_box['min_lon']

        lat_steps = int((max_lat - min_lat) / self.resolution) + 1
        lon_steps = int((max_lon - min_lon) / self.resolution) + 1

        for lat_step in range(lat_steps):
            start_lat = min_lat + lat_step * self.resolution
            stop_lat = start_lat + self.resolution
            if stop_lat > max_lat: 
                stop_lat = max_lat

            for lon_step in range(lon_steps):
                start_lon = min_lon + lon_step * self.resolution
                stop_lon = start_lon + self.resolution
                if stop_lon > max_lon:
                    stop_lon = max_lon

                coordinate_boxes.append({
                    'min_lat' : start_lat, 
                    'min_lon' : start_lon, 
                    'max_lat' : stop_lat,
                    'max_lon' : stop_lon
                })

        return coordinate_boxes

    def to_dict(self):
        ''' {@ineritDocs} '''
        return {
            'class' : 'simple_grid',
            'coordinate_box' : self.coordinate_box,
            'resolution' : self.resolution,
            'style_strategy' : self.style_strategy.to_dict()
        }

class SimpleGridStrategyFactory(PolygonStrategyFactory):
    ''' Simple grid strategy implementation of the PolygonStrategyFactory. '''

    def __init__(self, style_strategy_factory):
        '''
        Constructor.
 
        @param style_strategy_factory Interface for reconstructing the style strategy associated
        with the to be reconstructed polygon strategy
        @paramType StyleStrategyFactory
        @returns n/a
        '''
        assert style_strategy_factory is not None

        self.style_strategy_factory = style_strategy_factory

    def from_dict(self, state):
        return SimpleGridStrategy(
            state['coordinate_box'], state['resolution'],
            self.style_strategy_factory.from_dict(state['style_strategy'])
        )
