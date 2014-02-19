''' 
Interface definition of a strategy for breaking complex polygons, like counties, states, etc
into inscribed coordinate boxes which can be more easily used in analytic calculations.
'''

class PolygonStrategy:
    ''' Strategy for breaking complex polygons into inscribed coordinate boxes. '''

    def get_inscribed_boxes(self):
        '''
        @returns The coordinates boxes inscribed inside the complex polygon.
        @returnType List of dictionaries containing keys 'min_lat', 'max_lat', 'min_lon', 'max_lon'
        '''
        raise NotImplementedError()

    def to_dict(self):
        '''
        Serializes the polygon strategies state into a dictionary for later JSON encoding.
        
        @returns Dictionary serialization of this strategy. Will contain at least key 'class'
        which indicates the class of the serialized strategy for later reconstruction.
        @returnType dictionary
        '''
        raise NotImplementedError()

