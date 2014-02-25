''' Provides a common abstract style strategy factory interface into the various concrete style factories. '''

from smcity.styles.style_strategy import StyleStrategyFactory
from smcity.styles.heat_map_style_strategy import HeatMapStyleStrategyFactory

class AbstractStyleStrategyFactory(StyleStrategyFactory):
    ''' Delegates marshalling StyleStrategies to the appropriate factory implementation. '''

    def __init__(self, color_swatch_factory):
        ''' 
        Constructor. 

        @param color_swatch_factory Interface for reconstructing color swatches
        @paramType ColorSwatchFactory
        @returns n/a
        '''
        self.heat_map_factory = HeatMapStyleStrategyFactory(color_swatch_factory)

    def from_dict(self, state):
        ''' {@inheritDocs} '''
        assert 'class' in state.keys()

        if state['class'] == 'heat_map':
            return self.heat_map_factory.from_dict(state)
        else:
            raise Exception("Unknown style strategy '%s'!" % state['class'])
