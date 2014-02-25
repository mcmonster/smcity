''' Interface definition of a strategy that generates polygon styles based on analytics results. '''

class StyleStrategy:
    ''' Strategy for generating polygon styles based on analytics results. '''

    def prep_styling(self, results):
        '''
        Parses the entire result set to extract any needed data to control styling. For example,
        in generating heat map styling you'd need to know the max and min values. This function
        provides an opportunity for the StyleStrategy to extract this information.

        @param results Result set for whom styles are to be generated
        @paramType list of dictionaries containing at the least the key 'result'
        @returns n/a
        '''
        raise NotImplementedError()

    def style_result_geojson(self, result):
        '''
        Generates a GeoJSON polygon style for the provided result
       
        @param result Result to generate the style for
        @paramType dictionary containing at least the key 'result'
        @returns dictionary of geojson properties
        @returnType dictionary
        '''
        raise NotImplementedError()

    def to_dict(self):
        '''
        Serializes the style strategy state into a dictionary for later JSON encoding.

        @returns Dictionary serialization of this strategy. Will contain at least key 'class'
        which indicates the class of the serialized strategy for later reconstruction.
        @returnType dictionary
        '''
        raise NotImplementedError()

class StyleStrategyFactory:
    ''' Handles constructing and reconstructing the style strategy. '''
   
    def from_dict(self, state):
        '''
        Reconstructs the style strategy from its serialized state.

        @param state Serialized state of the style strategy
        @paramType dict
        @returns StyleStrategy
        '''
        raise NotImplementedError()
