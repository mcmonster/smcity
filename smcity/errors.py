''' Assorted exceptions. '''

class CreateError(Exception):
    ''' Exception thrown when unable to create a new database record. '''
    
    def __init__(self, message="Failed to create new database record!", orig_exception=None):
        Exception.__init__(self, message, orig_exceptio)

class ReadError(Exception):
    ''' Exception thrown when unable to retrieve a desired database record. '''
    
    def __init__(self, message="Failed to retrieve database record!", orig_exception=None):
        Exception.__init__(self, message, orig_exception)

class UpdateError(Exception):
    ''' Exception thrown when unable to update a database record. '''
 
    def __init__(self, message="Failed to update database record!", orig_exception=None):
        Exception.__init__(self, message, orig_exception)
