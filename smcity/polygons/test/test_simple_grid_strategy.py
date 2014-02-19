''' Unit tests for the SimpleGridStrategy class. '''

from smcity.polygons.simple_grid_strategy import SimpleGridStrategy

class TestSimpleGridStrategy:
    
    def test_get_inscribed_boxes(self):
        ''' Tests the function get_inscribed_boxes. '''
        coordinate_box = {'min_lat' : 0, 'min_lon' : 0, 'max_lat' : 1, 'max_lon' : 1}
        resolution = 0.6
        strategy = SimpleGridStrategy(coordinate_box, resolution)
 
        coordinate_boxes = strategy.get_inscribed_boxes()

        assert len(coordinate_boxes) == 4, len(coordinate_boxes)
        assert coordinate_boxes[0]['min_lat'] == 0, coordinate_boxes[0]['min_lat']
        assert coordinate_boxes[0]['min_lon'] == 0, coordinate_boxes[0]['min_lon']
        assert coordinate_boxes[0]['max_lat'] == 0.6, coordinate_boxes[0]['max_lat']
        assert coordinate_boxes[0]['max_lon'] == 0.6, coordinate_boxes[0]['max_lon']
        assert coordinate_boxes[1]['min_lat'] == 0, coordinate_boxes[1]['min_lat']
        assert coordinate_boxes[1]['min_lon'] == 0.6, coordinate_boxes[1]['min_lon']
        assert coordinate_boxes[1]['max_lat'] == 0.6, coordinate_boxes[1]['max_lat']
        assert coordinate_boxes[1]['max_lon'] == 1, coordinate_boxes[1]['max_lon']
        assert coordinate_boxes[2]['min_lat'] == 0.6, coordinate_boxes[2]['min_lat']
        assert coordinate_boxes[2]['min_lon'] == 0, coordinate_boxes[2]['min_lon']
        assert coordinate_boxes[2]['max_lat'] == 1, coordinate_boxes[2]['max_lat']
        assert coordinate_boxes[2]['max_lon'] == 0.6, coordinate_boxes[2]['max_lon']
        assert coordinate_boxes[3]['min_lat'] == 0.6, coordinate_boxes[3]['min_lat']
        assert coordinate_boxes[3]['min_lon'] == 0.6, coordinate_boxes[3]['min_lon']
        assert coordinate_boxes[3]['max_lat'] == 1, coordinate_boxes[3]['max_lat']
        assert coordinate_boxes[3]['max_lon'] == 1, coordinate_boxes[3]['max_lon']

    def test_to_dict(self):
        ''' Tests the to_dict function. '''
        coordinate_box = {'min_lat' : 0, 'min_lon' : 0, 'max_lat' : 1, 'max_lon' : 1}
        resolution = 0.6
        strategy = SimpleGridStrategy(coordinate_box, resolution)

        flattened = strategy.to_dict()

        assert flattened['class'] == 'simple_grid', flattened['class']
        assert flattened['resolution'] == 0.6, flattened['resolution']
        assert flattened['coordinate_box']['min_lat'] == 0, flattened['coordinate_box']['min_lat']
        assert flattened['coordinate_box']['min_lon'] == 0, flattened['coordinate_box']['min_lon']
        assert flattened['coordinate_box']['max_lat'] == 1, flattened['coordinate_box']['max_lat']
        assert flattened['coordinate_box']['max_lon'] == 1, flattened['cooridnate_box']['max_lon']
