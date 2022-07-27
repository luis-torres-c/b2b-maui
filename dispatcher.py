import argparse
import importlib
import os
import time
from datetime import datetime, timedelta

from core.fetcher import Fetcher
from utils.logger import logger
from utils.sentryio import sentryio

parser = argparse.ArgumentParser(description='Hans-Gruber Script')
parser.add_argument('--tracker', type=str)
parser.add_argument('--tracker-with-deps', dest='tracker_with_deps', type=str)
parser.add_argument('--initial-date', dest='initial_date', type=str)
parser.add_argument('--final-date', dest='final_date', type=str)
args = parser.parse_args()


CUSTOM_CLIENT_PATH = os.path.join(os.path.dirname(__file__), *['customfetcher'])
GENERIC_CLIENT_PATH = os.path.join(os.path.dirname(__file__), *['core', 'fetcher'])
NOT_ALLOWED_MODULES = ['__init__.py', '__pycache__', 'base.py']
# TODO check this list
NOT_ALLOWED_BASE_CLASSES = [
    'Fetcher',
    'DailyFetcher',
    'TodayFetcher',
    'PeriodicFetcher',
    'FileFetcher',
    'B2BWebFetcher',
    'MarketPlaceFetcher',
    'MarketPlaceFileFetcher',
    'FilePeriodFetcher']


def get_clients_by_names(names):
    listing = os.listdir(CUSTOM_CLIENT_PATH)
    for infile in listing:
        if infile.endswith('py') and infile not in NOT_ALLOWED_MODULES:
            infile = 'customfetcher.{}'.format(infile[:-3])
            importlib.import_module(infile)

    listing = os.listdir(CUSTOM_CLIENT_PATH)
    for infile in listing:
        if not infile.endswith('py') and infile not in NOT_ALLOWED_MODULES:
            listing_sub = os.listdir(CUSTOM_CLIENT_PATH + '/' + infile)
            for infile_sub in listing_sub:
                if infile_sub.endswith('py') and infile_sub not in NOT_ALLOWED_MODULES:
                    infile_sub = 'customfetcher.{}.{}'.format(infile, infile_sub[:-3])
                    importlib.import_module(infile_sub)

    listing = os.listdir(GENERIC_CLIENT_PATH)
    for infile in listing:
        if infile.endswith('py') and infile not in NOT_ALLOWED_MODULES:
            infile = 'core.fetcher.{}'.format(infile[:-3])
            importlib.import_module(infile)

    listing = os.listdir(GENERIC_CLIENT_PATH)
    for infile in listing:
        if not infile.endswith('py') and infile not in NOT_ALLOWED_MODULES:
            listing_sub = os.listdir(GENERIC_CLIENT_PATH + '/' + infile)
            for infile_sub in listing_sub:
                if infile_sub.endswith('py') and infile_sub not in NOT_ALLOWED_MODULES:
                    infile_sub = 'core.fetcher.{}.{}'.format(infile, infile_sub[:-3])
                    importlib.import_module(infile_sub)

    classes = _all_subclasses(Fetcher)
    valid_classes = [
        c for c in classes if c.__name__ not in NOT_ALLOWED_BASE_CLASSES]

    clients = []
    logger.debug('Seeking Trackers {}'.format(names))
    for name in names:
        for c in valid_classes:
            if c.name.lower() == name.lower():
                clients.append(c)

    logger.debug('Found Trackers {}'.format(list(map(lambda o: o.name, clients))))

    return clients


def _all_subclasses(cls):
    return cls.__subclasses__() + \
        [g for s in cls.__subclasses__() for g in _all_subclasses(s)]


def run(**kwargs):
    start_time = time.time()
    project_name = os.environ['PROJECT_NAME'].lower()
    if kwargs.get('tracker'):
        clients = get_clients_by_names([kwargs['tracker']])
    elif kwargs.get('tracker-with-deps'):
        client_on_a_list = get_clients_by_names([kwargs['tracker-with-deps']])
        clients = []
        for c in client_on_a_list:
            clients.append(c)
            clients.extend(c.related_fetchers)
    else:
        clients = get_clients_by_names(project_name.split(';'))

    for client in clients:
        logger.info('Starting Fetcher for {}'.format(client.name))
        start = datetime.now()
        try:
            client.dispatch(**kwargs)
        except Exception as e:
            # Handling exceptions to avoid dependency among clients if one fails
            logger.critical(e, exc_info=True)
            sentryio.catch_exception(e)
        done = datetime.now()
        elapsed = (done - start).total_seconds()
        logger.info('Ending Fetcher for {} in {} seconds'.format(client.name, elapsed))
    logger.info('{} finished in {} secs'.format(' '.join([c.name for c in clients]), time.time() - start_time))


if __name__ == '__main__':
    '''
        Execute dispatcher as script with custom parameters
        python dispatcher.py --tracker <tracker name> --initial-date <date format %Y-%M-%d>
            --final-date <date format %Y-%M-%d>
        otherwise, without custom parameters.
        python dispatcher.py
    '''
    parameters = {
        'tracker': args.tracker,
        'tracker-with-deps': args.tracker_with_deps,
        'initial_date': args.initial_date,
        'final_date': args.final_date,
    }
    logger.debug('Using dispatcher\'s parameters {}'.format(parameters))
    run(**parameters)
