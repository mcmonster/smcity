''' Concrete polygon strategy factory. '''

from smcity.polygons.simple_grid_strategy import SimpleGridStrategyFactory

class PolygonStrategyFactory:
    ''' Marshalls PolygonStrategies from the serialized dictionary forms. ''' 
 
    def __init__(self):
        ''' Constructor. '''
        self.simple_grid_factory = SimpleGridStrategyFactory()

    def from_dict(self, state):  
        '''
        Marshalls the provided serialized PolygonStrategy.

        @param state Serialized state of the PolygonStrategy to be marshalled.
        @paramType dictionary with at least key 'class'
        @returns Marshalled PolygonStrategy
        @returnType PolygonStrategy
        ''' 
        assert 'class' in state.keys()
 
        if state['class'] == 'simple_grid':
            return self.simple_grid_factory.from_dict(state)
        else:
            raise Exception("Unknown polygon stategy '%s'!" % state['class'])

