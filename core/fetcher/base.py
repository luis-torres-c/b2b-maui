import os
import datetime
import calendar
import pytz

from utils.locker import Locker
from utils.tracker import Tracker, DailyTracker
from utils.periods import Period
from utils.processedfiles import ProcessedFiles
from utils.external_configurations import get_external_configurations
from utils.logger import logger
from core.utils import datetime_utc_to_timezone, datetime_tz_to_utc, save_state
from core.connectors.b2b.utils import B2BTimeoutRequestError
from core.connectors.b2b.utils import BBrEcommerceErrorClientInPortal
from core.connectors.b2b.utils import ConnectorB2BClientInPortalError
from core.connectors.b2b.utils import ConnectorB2BLoginErrorConnection
from core.connectors.b2b.utils import CredentialWithInsufficientAccessError
from core.connectors.b2b.utils import NotCredentialsProvidedError
from core.connectors.b2b.utils import WalmartReportWaitingTimeExceded
from core.connectors.b2b.utils import ConnectorB2BInformationNotAvailable
from core.fetcher.mixins import TimezonesAndSchedulesMixIn
from collections import namedtuple


class Fetcher:

    @property
    def name(self):
        raise NotImplementedError

    @classmethod
    def fetch(cls, **kwargs):
        raise NotImplementedError

    @classmethod
    def settings(cls):
        raise NotImplementedError

    @classmethod
    def dispatch(cls, **kwargs):
        logger.info("Getting external configurations")
        dashboard = os.environ.get('DASHBOARD_NAME', None)
        configurations = get_external_configurations(dashboard, cls.name) if dashboard else dict()
        parameters = {**kwargs, **cls.settings(), **configurations}
        if Locker.acquire(cls.name):
            try:
                cls.fetch(**parameters)
                save_state(cls.name, status='OK')
            except ConnectorB2BLoginErrorConnection:
                logger.warning("Credenciales Incorrectas")
                save_state(cls.name, status='ERROR - Credenciales Incorrectas')
            except WalmartReportWaitingTimeExceded:
                logger.warning("El portal no pudo generar el informe en los tiempos esperados")
                save_state(cls.name, status='Warning - Informe no pudo ser generado desde el portal, tiempo excedido')
            except (BBrEcommerceErrorClientInPortal, ConnectorB2BClientInPortalError):
                logger.warning("Se encuentra un usuario utilizando la credencial")
                save_state(cls.name, status='Warning - El portal se encuentra con un usuario logueado')
            except (NotCredentialsProvidedError, CredentialWithInsufficientAccessError):
                logger.warning("No hay credenciales disponibles")
                save_state(cls.name, status='ERROR - No hay credenciales')
            except B2BTimeoutRequestError:
                logger.warning("Error de timeout en consulta")
                save_state(cls.name, status='ERROR - Timeout sobrepasado')
            except ConnectorB2BInformationNotAvailable:
                logger.warning("El informe de ventas todavía no está listo")
                save_state(cls.name, status='Warning - Informe no pudo ser obtenido desde el portal, temporalmente desabilitado')
            except Exception:
                logger.warning("Error indeterminado")
                save_state(cls.name, status='ERROR - Error indeterminado')
                raise
            finally:
                Locker.release(cls.name)
        else:

            logger.warning('Locked {}'.format(cls.name))


class RelatedFetcherMixIn:
    related_fetchers = []


class B2BWebFetcher(RelatedFetcherMixIn, Fetcher):

    DAYS_TO_CONSOLIDATE = 7

    # process must return all 'ready' periods
    @classmethod
    def process(cls, **kwargs):
        raise NotImplementedError

    @classmethod
    def settings(cls):
        raise NotImplementedError

    @classmethod
    def generate_tag(cls, **kwargs):
        name = kwargs['tracker']
        username = kwargs['b2b_username']
        empresa = kwargs['b2b_empresa'] if 'b2b_empresa' in kwargs else ''
        return '{}{}{}'.format(name, username, empresa).encode('utf-8')

    # This return a datetime objects that represent the last day to consider in periods
    @classmethod
    def last_date_to_consider(cls):
        return datetime.date.today() - datetime.timedelta(days=1)

    @classmethod
    def get_periods(cls, tag, **kwargs):
        date_format = '%Y-%m-%d'
        tag = cls.generate_tag(**kwargs)
        last_day = cls.last_date_to_consider()

        # Check if historical request
        if kwargs.get('initial_date') and kwargs.get('final_date'):
            logger.info("Adding period from {} to {}".format(kwargs.get('initial_date'), kwargs.get('final_date')))
            Period.insert_period(tag, kwargs.get('initial_date'), kwargs.get('final_date'), 'empty')
        elif kwargs.get('initial_date') and not kwargs.get('final_date'):
            logger.info("Adding period from {} to yesterday".format(kwargs.get('initial_date')))
            Period.insert_period(tag, kwargs.get('initial_date'), last_day.strftime(date_format), 'empty')
        else:
            # Check if last days are tagged
            last_date_tagged = Period.last_date_tagged(tag)
            datetime_last_date = datetime.datetime.strptime(last_date_tagged, date_format)
            if datetime_last_date.date() < last_day:
                logger.info("Adding period from {} to {}".format(last_date_tagged, last_day.strftime(date_format)))
                Period.insert_period(tag, last_date_tagged, last_day.strftime(date_format), 'empty')

        # get all periods that are empty
        empty_periods = Period.select_not_processed(tag)
        if not empty_periods:
            final_consolidate_date = last_day
            initial_consolidate_date = final_consolidate_date - datetime.timedelta(days=cls.DAYS_TO_CONSOLIDATE)
            Period.insert_period(tag, initial_consolidate_date.strftime(date_format), final_consolidate_date.strftime(date_format), 'empty')
            empty_periods = Period.select_not_processed(tag)
        return empty_periods

    @classmethod
    def insert_period(cls, _tag, _from, _to, _status):
        logger.info(f"Saving period {_from} to {_to} whit status {_status}")
        Period.insert_period(_tag, _from, _to, _status)

    @classmethod
    def valid_credentials(cls, **kwargs):
        return kwargs.get('b2b_username', False)

    @classmethod
    def fetch(cls, **kwargs):
        if not cls.valid_credentials(**kwargs):
            logger.warning("There are no credentials to use on this tracker")
            return
        tag = cls.generate_tag(**kwargs)
        empty_periods = cls.get_periods(**kwargs)

        # iterate from every period requesting files
        complete_periods = []
        for period in empty_periods:
            args = {**kwargs, **period}
            instance = cls()
            # instance return all periods that contains status changes, must be a list of dicts
            logger.info("Processing period {} to {} from tag {}".format(args['from'], args['to'], args['tag']))
            periods = instance.process(**args)
            complete_periods += periods

        for period in complete_periods:
            cls.insert_period(tag, period['from'], period['to'], period['status'])


class FilePeriodFetcher(RelatedFetcherMixIn, Fetcher):

    # process must return all 'ok' periods
    @classmethod
    def process(cls, **kwargs):
        raise NotImplementedError

    @classmethod
    def settings(cls):
        raise NotImplementedError

    @classmethod
    def generate_tag(cls, **kwargs):
        raise NotImplementedError

    @classmethod
    def post_process(cls, data):
        return data

    @classmethod
    def consolidate_data(cls, processed_data, **kwargs):
        raise NotImplementedError

    @classmethod
    def fetch(cls, **kwargs):
        tag = cls.generate_tag(**kwargs)

        # get all periods that are ready
        empty_periods = Period.select_ready_to_process(tag)

        # iterate from every period processing files
        complete_periods = []
        for period in empty_periods:
            initial_date = datetime.datetime.strptime(period['from'], '%Y-%m-%d').date()
            final_date = datetime.datetime.strptime(period['to'], '%Y-%m-%d').date()
            while initial_date <= final_date:
                day = initial_date.strftime('%Y-%m-%d')
                dates = {'from': day, 'to': day}
                args = {**kwargs, **period, **dates}
                instance = cls()
                instance.actual_date_tz = None
                instance.actual_date = datetime.date.today()
                logger.info("Processing period {} to {} from tag {}".format(args['from'], args['to'], args['tag']))
                periods, processed_data = instance.process(**args)
                complete_periods += periods
                if processed_data:
                    logger.info('Starting post process')
                    processed_data = instance.post_process(processed_data)
                    logger.info('Starting data consolidation')
                    instance.consolidate_data(processed_data, **kwargs)
                else:
                    logger.warning('No Data for this period')
                    logger.debug(f'Period {periods}')
                initial_date += datetime.timedelta(days=1)

        for period in complete_periods:
            logger.info("Saving period {} to {} whit status {}".format(period['from'], period['to'], period['status']))
            Period.insert_period(tag, period['from'], period['to'], period['status'])


class SignalFetcher:
    post_save_observables = []

    def post_save(self, func):
        self.post_save_observables.append(func)

    def notify_post_save(self):
        # Improve this
        for obs in self.post_save_observables:
            logger.info('Executing Function {} Args {} Keywords {}'.format(obs.func.__name__, obs.args, obs.keywords))
            obs()
        self.post_save_observables.clear()


class PeriodicFetcher(RelatedFetcherMixIn, Fetcher):
    WAIT_FOR_DATA = True

    def __init__(self, actual_date=None, **kwargs):
        self._actual_date = actual_date
        self.signal = SignalFetcher()

    def post_save(self, func):
        self.signal.post_save(func)

    @property
    def actual_date(self):
        return self._actual_date

    @actual_date.setter
    def actual_date(self, actual_date):
        self._actual_date = actual_date

    def consolidate_data(self, data, **kwargs):
        raise NotImplementedError

    def process(self, **kwargs):
        raise NotImplementedError

    def post_process(self, processed_data, **kwargs):
        return processed_data


class DailyFetcher(TimezonesAndSchedulesMixIn, PeriodicFetcher):

    # TODO change this bad variable name
    HOURS_DELAY = 0
    PROCESS_BY_RANGE_DAYS_ENABLE = True
    START_IN_LAST_YEAR = False

    @classmethod
    def fetch(cls, **kwargs):

        timezone_name = kwargs.get('timezone_name')

        track = Tracker.select_track(cls.name)
        if not track:
            logger.info('Creating Tracker for {}'.format(cls.name))
            dt = datetime.datetime.utcnow()
            if cls.START_IN_LAST_YEAR:
                dt = datetime.datetime(dt.year - 1, 1, 1)
                logger.info('Setting first execution to {}'.format(dt.strftime('%Y-%m-%d')))
            if timezone_name:
                dt = datetime_utc_to_timezone(dt, timezone_name)
            _, ts_initial = DailyTracker.create_track(cls.name, dt.date())
        else:
            ts_initial, _ = track

        if kwargs.get('initial_date'):
            datetime_initial_utc = datetime.datetime.strptime(kwargs.get('initial_date'), '%Y-%m-%d')
        else:
            datetime_initial_utc = datetime.datetime.utcfromtimestamp(ts_initial)
        # Fixing time to 23:59:59 to avoid issue with timezone
        datetime_initial_utc = datetime.datetime.combine(datetime_initial_utc.date(), datetime.time.max)

        # TODO change hour delay logic
        hours_delay = cls.HOURS_DELAY
        range_days = kwargs.get('range_days')
        if range_days:
            if not cls.PROCESS_BY_RANGE_DAYS_ENABLE:
                logger.info('Process by range days is disable for {}'.format(cls.name))
                return
            hours_delay = 0
            datetime_initial_utc = datetime_initial_utc - datetime.timedelta(days=range_days)

        if kwargs.get('final_date'):
            datetime_final_utc = datetime.datetime.strptime(kwargs.get('final_date'), '%Y-%m-%d')
        else:
            datetime_final_utc = datetime.datetime.utcnow() - datetime.timedelta(hours=hours_delay)

        if timezone_name:
            datetime_initial = datetime_utc_to_timezone(datetime_initial_utc, timezone_name)
            datetime_final = datetime_utc_to_timezone(datetime_final_utc, timezone_name)
            logger.info('Processing {} to {} Timezone {}'.format(
                datetime_initial.date(), datetime_final.date(), timezone_name
            ))

        else:
            datetime_initial = datetime_initial_utc
            datetime_final = datetime_final_utc
            logger.info('Processing {} to {} UTC'.format(datetime_initial.date(), datetime_final.date()))

        instance = cls()
        while datetime_initial.date() <= datetime_final.date():
            # Set the actual processing date each iteration
            if timezone_name:
                dt_tz = datetime_initial
                instance.actual_datetime_tz = dt_tz
                instance.actual_date_tz = dt_tz.date()
                instance.actual_date = datetime_tz_to_utc(dt_tz).date()
                instance.actual_datetime = datetime_tz_to_utc(dt_tz)
            else:
                instance.actual_datetime_tz = None
                instance.actual_date_tz = None
                instance.actual_datetime = datetime_initial
                instance.actual_date = datetime_initial.date()

            logger.info('Processing {} {}'.format(
                instance.actual_date, 'Timezone {}'.format(timezone_name) if timezone_name else 'UTC'))

            processed_data = instance.process(**kwargs)
            if processed_data:
                logger.info('Starting post process')
                processed_data = instance.post_process(processed_data)
                logger.info('Starting data consolidation')
                instance.consolidate_data(processed_data, **kwargs)
            else:
                if cls.WAIT_FOR_DATA:
                    # Nothing more to do. Stopping process until new data appears for that date
                    # We assume that there is no data for variable datetime_initial
                    datetime_final = datetime_initial
                else:
                    logger.warning('No Data for {}'.format(datetime_initial.date()))

            enable_tracker = not (kwargs.get('initial_date') and kwargs.get('final_date'))
            if enable_tracker:
                if timezone_name:
                    dt_utc = datetime_initial.astimezone(pytz.utc)
                    logger.info('Saving Track {} UTC'.format(dt_utc))
                    ts_processed_day = calendar.timegm(dt_utc.timetuple())
                else:
                    logger.info('Saving Track {} UTC'.format(datetime_initial))
                    ts_processed_day = calendar.timegm(datetime_initial.timetuple())

                Tracker.update_track(
                    cls.name,
                    ts_processed_day,
                )
            else:
                logger.info('Disable Updating Tracker. - Running fetcher on the fly')

            # Trigger postSave functions and reset it to next date iteration.
            instance.signal.notify_post_save()
            datetime_initial = datetime_initial + datetime.timedelta(days=1)


class FileFetcher(Fetcher):

    ROOT_FOLDER = ''

    def consolidate_data(self, data, **kwargs):
        raise NotImplementedError

    def process(self, **kwargs):
        raise NotImplementedError

    @classmethod
    def fetch(cls, **kwargs):

        track = Tracker.select_track(cls.name)
        if not track:
            logger.info('Not Tracker ...')
            return
        ts_track, _ = track

        instance = cls()
        timezone_name = kwargs.get('timezone_name')
        base_path = os.path.join(kwargs['local_path_repository'], instance.ROOT_FOLDER)
        Pair = namedtuple("Pair", ["name", "mod_time"])
        file_and_mod_time = [Pair(file, os.path.getmtime(os.path.join(base_path, file))) for file in os.listdir(base_path)]
        sorted_files = sorted(file_and_mod_time, key=lambda x: x.mod_time)

        for file in sorted_files:
            actual_file = file.name
            if ProcessedFiles.is_procesed_file(actual_file, cls.name):
                continue

            actual_datetime = datetime.datetime.utcnow()
            if timezone_name:
                dt_tz = datetime_utc_to_timezone(actual_datetime, timezone_name)
                instance.actual_datetime_tz = dt_tz
                instance.actual_date_tz = dt_tz.date()
                instance.actual_date = actual_datetime.date()
                instance.actual_datetime = actual_datetime
            else:
                instance.actual_datetime_tz = None
                instance.actual_date_tz = None
                instance.actual_datetime = actual_datetime
                instance.actual_date = actual_datetime.date()

            logger.info('Processing {}'.format(actual_file))
            f_dict = {'file': actual_file}
            processed_data = instance.process(**kwargs, **f_dict)
            if processed_data:
                logger.info('Starting data consolidation')
                instance.consolidate_data(processed_data, **kwargs)
            logger.info('Saving Track {}'.format(actual_file))

            Tracker.update_track(
                cls.name,
                int(actual_datetime.timestamp()),
            )
            ProcessedFiles.set_processed_file(actual_file, cls.name)


class TodayFetcher(DailyFetcher):

    @classmethod
    def fetch(cls, **kwargs):

        timezone_name = kwargs.get('timezone_name')

        date_time = datetime.datetime.combine(datetime.datetime.utcnow(), datetime.time.max)
        ts_initial = calendar.timegm(date_time.timetuple())

        datetime_initial_utc = datetime.datetime.utcfromtimestamp(ts_initial)
        datetime_initial_utc = datetime.datetime.combine(datetime_initial_utc.date(), datetime.time.max)
        datetime_final_utc = datetime.datetime.combine(datetime_initial_utc.date(), datetime.time.max)

        if timezone_name:
            datetime_initial = datetime_utc_to_timezone(datetime_initial_utc, timezone_name)
            datetime_final = datetime_utc_to_timezone(datetime_final_utc, timezone_name)
            logger.info('Processing {} to {} Timezone {}'.format(
                datetime_initial.date(), datetime_final.date(), timezone_name
            ))

        else:
            datetime_initial = datetime_initial_utc
            datetime_final = datetime_final_utc
            logger.info('Processing {} to {} UTC'.format(datetime_initial.date(), datetime_final.date()))

        instance = cls()
        if timezone_name:
            dt_tz = datetime_initial
            instance.actual_datetime_tz = dt_tz
            instance.actual_date_tz = dt_tz.date()
            instance.actual_date = datetime_tz_to_utc(dt_tz).date()
            instance.actual_datetime = datetime_tz_to_utc(dt_tz)
        else:
            instance.actual_datetime_tz = None
            instance.actual_date_tz = None
            instance.actual_datetime = datetime_initial
            instance.actual_date = datetime_initial.date()

        logger.info('Processing {} {}'.format(
            instance.actual_date, 'Timezone {}'.format(timezone_name) if timezone_name else 'UTC'))

        processed_data = instance.process(**kwargs)
        if processed_data:
            logger.info('Starting post process')
            processed_data = instance.post_process(processed_data)
            logger.info('Starting data consolidation')
            instance.consolidate_data(processed_data, **kwargs)
        else:
            logger.warning('No Data for {}'.format(datetime_initial.date()))

        instance.signal.notify_post_save()
