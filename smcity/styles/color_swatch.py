''' Encapsulates a set of color values that can be assigned to style polygons. '''

class ColorSwatch:
    ''' Encapsulates a set of color values that can be assigned to style polygons. '''

    def __init__(self, colors):
        '''
        Constructor.

        @param colors Colors to use
        @paramType list of hex color specifications (#XXXXXX)
        @returns n/a
        '''
        assert len(colors) > 0, len(colors)
 
        self.colors = colors

    def get_color(self, index):
        '''
        @param index Position in the swatch whose color we want
        @paramType int
        @returns Color at that index position
        @returnType string/hex color notation (#XXXXXX)
        '''
        assert 0 <= index and index < len(self.colors), index

        return self.colors[index]

    def get_num_colors(self):
        '''
        @returns # of colors in this swatch
        @returnType int
        '''
        return len(self.colors)

    def to_dict(self):
        '''
        @returns This object flattened into a dictionary for later JSON encoding
        @returnType dictionary with at least key 'class'
        '''
        return {
            'class' : 'color_swatch',
            'colors' : self.colors
        }

class ColorSwatchFactory:
    ''' Handles marshalling color swatches from their flattened states. '''
  
    def from_dict(self, state):
        '''
        Marshals the color swatch from the provided flattened state.

        @param state Flattened state of the color swatch
        @paramType dictionary with keys 'class', 'colors'
        @returns Marshalled color swatch
        @returnType ColorSwatch
        '''
        assert state['class'] == 'color_swatch', state['class']

        return ColorSwatch(state['colors'])
