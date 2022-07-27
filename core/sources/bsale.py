import calendar
import datetime

from core.sources.base import Source
from core.connectors.bsale import BSaleConnector

from utils.logger import logger


class BSaleSource(Source):

    CONNECTOR = BSaleConnector

    def parse_data(self, connector, **kwargs):
        raise NotImplementedError

    def process(self, **kwargs):
        tokens = kwargs['tokens']
        initial_day = self.actual_date_tz

        datetime_start = datetime.datetime.combine(
            initial_day, datetime.time.min)
        datetime_end = datetime.datetime.combine(
            initial_day, datetime.time.max)
        ts_start = calendar.timegm(datetime_start.utctimetuple())
        ts_end = calendar.timegm(datetime_end.utctimetuple())

        all_data = []

        args = {
            'timestamp_start': ts_start,
            'timestamp_end': ts_end,
        }

        additional_args = {
            'is_multi_tokens': len(tokens) > 1,  # This bsale project contains multiples tokens.
        }
        parser_args = {**kwargs, **additional_args}

        for token in tokens:
            logger.debug('Token {}'.format(token))
            args.update({'token': token})
            conn = self.CONNECTOR.get_instance(**args)
            all_data.extend(self.parse_data(conn, **parser_args))

        return all_data
