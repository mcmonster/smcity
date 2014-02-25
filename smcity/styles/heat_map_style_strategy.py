''' Generates heat map styling for polygons in a result set. '''

from smcity.logging.logger import Logger
from smcity.styles.style_strategy import StyleStrategy, StyleStrategyFactory

logger = Logger(__name__)

class HeatMapStyleStrategy(StyleStrategy):
    ''' Generates heap map styling for polygons in a result set. '''

    def __init__(self, color_swatch, min_value=None, max_value=None):
        '''
        Constructor.

        @param color_swatch Provides colors to use in generating the heat map
        @paramType ColorSwatch
        @param min_value Min value to use when heat coloring the individual results
        @paramType numeric
        @param max_value Max value to use when heat coloring the individual results
        @paramType numeric
        @returns n/a
        '''
        self.color_swatch = color_swatch
        self.min_value = min_value
        self.max_value = max_value

    def prep_styling(self, results):
        ''' {@inheritDocs} '''
        self.min_value = 99999999
        self.max_value = -99999999

        for result in results:
            if result['result'] < self.min_value:
                self.min_value = result['result']
            if result['result'] > self.max_value:
                self.max_value = result['result']

        logger.debug("Min: %s, Max: %s", self.min_value, self.max_value)

    def style_result_geojson(self, result):
        ''' {@inheritDocs} '''
        assert self.min_value is not None, "Must call prep_styling() first!"
        assert self.max_value is not None, "Must call prep_styling() first!"

        value_range = float(self.max_value - self.min_value)
        heat_index = 0
        if value_range > 0:
            print "Result: ", result['result']
            print "ValueRange: ", value_range
            print "Ratio: ", (result['result'] / value_range)
            print "# Colors: ", self.color_swatch.get_num_colors()
            heat_index = int((result['result'] / value_range) * (self.color_swatch.get_num_colors() - 1))
        heat_color = self.color_swatch.get_color(heat_index)

        return {'fill' : heat_color}

    def to_dict(self):
        ''' {@inheritDocs} '''
        return {
            'class' : 'heat_map',
            'color_swatch' : self.color_swatch.to_dict(),
            'min_value' : self.min_value,
            'max_value' : self.max_value
        }

class HeatMapStyleStrategyFactory(StyleStrategyFactory):
    ''' Handles reconstructing this style strategy from its flattened state. '''

    def __init__(self, color_swatch_factory):
        '''
        Constructor.
 
        @param color_swatch_factory Interface for marshalling color swatches
        @paramType ColorSwatchFactory
        @returns n/a 
        '''
        assert color_swatch_factory is not None
    
        self.color_swatch_factory = color_swatch_factory

    def from_dict(self, state):
        ''' {@inheritDocs} '''
        assert state['class'] == 'heat_map', "State is not of heap map style strategy (%s)" % state['class']

        return HeatMapStyleStrategy(
            self.color_swatch_factory.from_dict(state['color_swatch']), 
            state['min_value'], state['max_value']
        )
        
