import logging
import os
import sys
import datetime

from kp_fraydit.datetime_functions import today_as_string
from kp_fraydit.root import root_dir

# setup the logger
logger = logging.getLogger(__name__)

# define file handler and set formatter
current_directory = os.path.dirname(os.path.abspath(__file__))

file_handler = logging.FileHandler(f'{root_dir}/logs/errors_{today_as_string()}.log')
formatter    = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)

# add file handler to logger
logger.addHandler(file_handler)

# Set logger level to error
logger.setLevel(logging.ERROR)

class CustomError(Exception):
    def __init__(self, *args):
        if args:
            self.message = args[0]
            if len(args) > 1: self.error_type = args[1]
        else:
            self.message = None

        print(self)
    
    def __str__(self):
    
        if hasattr(self, 'message') and hasattr(self, 'error_type'):
            return f'CustomError: {self.error_type}: {self.message}'
        elif hasattr(self, 'message'):
            return f'CustomError: {self.message}'
        else:
            return 'CustomError has been raised'


class OfflineError(CustomError):
    def __init__(self):
        self.message = f'Kafka is unreachable'
        self.error_type = 'OfflineError'
        super().__init__(self.message, self.error_type)
        logger.error(self.message)


class BrokerOfflineError(CustomError):
    def __init__(self, address):
        self.address = address
        self.message = f'Kafka broker unreachable at address: {self.address}'
        self.error_type = 'BrokerOfflineError'
        super().__init__(self.message, self.error_type)
        logger.error(self.message)


class RegistryOfflineError(CustomError):
    def __init__(self, address):
        self.address = address
        self.message = f'Kafka registry unreachable at address: {self.address}'
        self.error_type = 'RegistryOfflineError'
        super().__init__(self.message, self.error_type)
        logger.error(self.message)