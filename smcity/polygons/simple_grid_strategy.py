''' Simple arbitrary resolution grid strategy. '''

from smcity.polygons.polygon_strategy import PolygonStrategy, PolygonStrategyFactory

class SimpleGridStrategy(PolygonStrategy):
    ''' Simple arbitrary resolution grid strategy. '''

    def __init__(self, coordinate_box, resolution):
        '''
        Constructor. 
 
        @param coordinate_box Coordinate box which should be broken down into sub-boxes of size resolution
        @paramType dictionary with keys 'min_lat', 'max_lat', 'min_lon', 'max_lon'
        @param resolution resolution of the sub-boxes in degrees
        @paramType float
        @returns n/a
        '''
        assert 'min_lat' in coordinate_box.keys()
        assert 'max_lat' in coordinate_box.keys()
        assert 'min_lon' in coordinate_box.keys()
        assert 'max_lon' in coordinate_box.keys()
        assert resolution > 0

        self.coordinate_box = coordinate_box
        self.resolution = resolution

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
            'resolution' : self.resolution
        }

class SimpleGridStrategyFactory(PolygonStrategyFactory):
    def from_dict(self, state):
        return SimpleGridStrategy(state['coordinate_box'], state['resolution'])
