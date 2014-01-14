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
        self.logger = logging.getLogger(module)

    def debug(self, message, *args):
        ''' Logs debug level messages. '''
        if len(args) > 0:
            self.logger.debug(self.hostname + ' ' + message, *args)
        else:
            self.logger.debug(self.hostname + ' ' + message)

    def error(self, message, *args):
        ''' Logs error level messages. '''
        if len(args) > 0:
            self.logger.error(self.hostname + ' ' + message, *args)
        else:
            self.logger.error(self.hostname + ' ' + message)

    def exception(self):
        ''' Logs exception messages. '''
        self.logger.exception(self.hostname + ' Exception occurred!')

    def info(self, message, *args):
        ''' Logs info level messages. '''
        if len(args) > 0:
            self.logger.info(self.hostname + ' ' + message, *args)
        else:
            self.logger.info(self.hostname + ' ' + message)

    def warn(self, message, *args):
        ''' Logs warn level messages. '''
        if len(args) > 0:
            self.logger.warn(self.hostname + ' ' + message, *args)
        else:
            self.logger.warn(self.hostname + ' ' + message)

