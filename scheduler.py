import os
import signal

from apscheduler.schedulers.blocking import BlockingScheduler

from dispatcher import run as dispatch
from utils.logger import logger
from utils.sentryio import sentryio


class ConfigException(Exception):
    pass


def clean_up(signalnum, frame):
    from utils.locker import Locker
    client_names = os.environ['PROJECT_NAME'].split(';')
    logger.warning('Signal {} received: Cleaning up hans-gruber'.format(signalnum))
    for client_name in client_names:
        Locker.release(client_name)
        logger.info('Locker removed for {}'.format(client_name))

    logger.info('Cleaning SentryIO')
    sentryio.clean_gracefully()
    logger.warning('Cleaning is done.')


signal.signal(signal.SIGTERM, clean_up)


def initialize():
    # SQLITE3 Creation Tables
    from utils.wsqlite3 import SQLiteManager
    SQLiteManager.initialize_db()


class FormatCronParser:
    def __init__(self, cron_schedule):
        self.minute, self.hour, self.day, self.month, self.day_of_week, self.consolidate_days = cron_schedule.split(' ')
        self.consolidate_days = int(self.consolidate_days)


def run():

    initialize()

    if os.environ.get('FREQUENCY_DATA_EXTRACTION'):
        scheduler = BlockingScheduler()
        cron_params = os.environ['FREQUENCY_DATA_EXTRACTION'].split(';')
        for cron_param in cron_params:
            cron_schedule = FormatCronParser(cron_param)
            logger.info(f'Adding cron schedule whith cron setup {cron_param}')
            if cron_schedule.consolidate_days != 1:
                args = {'range_days': cron_schedule.consolidate_days}
            else:
                args = {}
            scheduler.add_job(
                dispatch,
                'cron',
                kwargs=args,
                minute=cron_schedule.minute,
                hour=cron_schedule.hour,
                day=cron_schedule.day,
                month=cron_schedule.month,
                day_of_week=cron_schedule.day_of_week,
                max_instances=2
            )

        try:
            logger.info('The scheduled work will start now')
            scheduler.start()
        except (KeyboardInterrupt, SystemExit, InterruptedError) as e:
            logger.critical(f'Something went wrong on scheduler {e}', exc_info=True)

    else:
        logger.warning('There is not fetching data schedule variable')
        return


if __name__ == '__main__':
    run()
