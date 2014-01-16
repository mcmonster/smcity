''' Contains a logger which automatically appends additional metadata to log entries. '''

import logging
import subprocess

class Logger:
    ''' Amazon Web Services friendly architecture wide logger. '''

    def __init__(self, module):
        '''
        Constructor.

        @param module Name of the module this Logger belongs to.
        @paramType string
        @returns n/a
        '''
        assert module is not None, "module not be None!"

        self.hostname = subprocess.check_output(
            'ec2metadata --instance-id', shell=True
        ).rstrip()
        self.module = module

    def debug(self, message, *args):
        ''' Logs debug level messages. '''
        if len(args) > 0:
            logging.debug(self.hostname + ' ' + message, *args)
        else:
            logging.debug(self.hostname + ' ' + message)

    def error(self, message, *args):
        ''' Logs error level messages. '''
        if len(args) > 0:
            logging.error(self.hostname + ' ' + message, *args)
        else:
            logging.error(self.hostname + ' ' + message)

    def exception(self):
        ''' Logs exception messages. '''
        logging.exception(self.hostname + ' Exception occurred!')

    def info(self, message, *args):
        ''' Logs info level messages. '''
        if len(args) > 0:
            logging.info(self.hostname + ' ' + message, *args)
        else:
            logging.info(self.hostname + ' ' + message)

    def warn(self, message, *args):
        ''' Logs warn level messages. '''
        if len(args) > 0:
            logging.warn(self.hostname + ' ' + message, *args)
        else:
            logging.warn(self.hostname + ' ' + message)

