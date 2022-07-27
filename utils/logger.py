import logging
import sys
import os

DEFAULT_LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL') or logging.INFO


def initialize_simple_logger():
    logging_level = DEFAULT_LOGGING_LEVEL
    logger = logging.getLogger('Integrations')
    logger.setLevel(logging_level)
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter('[LEVEL:%(levelname)s] %(asctime)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


logger = initialize_simple_logger()
