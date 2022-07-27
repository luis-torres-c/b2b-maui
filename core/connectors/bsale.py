from urllib.parse import (
    urlencode,
    urlunparse,
    parse_qsl,
    urlparse,
)

from core.utils import makes_request
from core.connectors import Connector
from utils.logger import logger


class BSaleURL:
    SCHEME_URL = 'https'
    BASE_URL = 'api.bsale.cl'
    DOCUMENTS_API = '/v1/documents.json'
    STOCKS_API = '/v1/stocks.json'
    PAYMENTS_API = '/v1/payments.json'
    VARIANTS_API = '/v1/variants.json'
    PRICES_API = '/v1/price_lists/{}/details.json'
    EXPAND_NAME = 'expand'
    LIMIT_NAME = 'limit'
    DEFAULT_LIMIT_VALUE = 50
    STATE = 'state'
    STATE_VALUE = 0
    EMISSION_DATE_RANGE_NAME = 'emissiondaterange'
    RECORD_DATE = 'recorddate'

    def __init__(self, ts_start, ts_end):
        self.timestamp_start = ts_start
        self.timestamp_end = ts_end

    def make_url(
            self,
            api_path='',
            url='',
            expanded_fields=(),
            limit=0
    ):
        if url:
            p = urlparse(url)
            api_path = p.path
            query = {pair[0]: pair[1] for pair in parse_qsl(p.query)}
        else:
            query = {}

        if limit:
            query.update({self.LIMIT_NAME: limit})

        if expanded_fields:
            query.update(
                {self.EXPAND_NAME: '[{}]'.format(
                    ','.join(expanded_fields)
                )}
            )

        if api_path == self.DOCUMENTS_API:
            query.update(
                {self.EMISSION_DATE_RANGE_NAME: '[{},{}]'.format(
                    self.timestamp_start,
                    self.timestamp_end,
                )}
            )
        elif api_path == self.PAYMENTS_API:
            query.update(
                {self.RECORD_DATE: '{}'.format(
                    self.timestamp_start,
                )}
            )
        if api_path in [self.PAYMENTS_API, self.DOCUMENTS_API]:
            query.update(
                {
                    self.STATE: '{}'.format(self.STATE_VALUE)
                }
            )

        q = urlencode(query)
        url_parts = (
            self.SCHEME_URL,  # scheme
            self.BASE_URL,  # netloc
            api_path,  # path
            '',  # params
            q,  # query
            '',  # fragments
        )

        return urlunparse(url_parts)


class BSaleConnector(Connector):

    @classmethod
    def get_instance(cls, **kwargs):
        token = kwargs['token']
        initial_ts = kwargs['timestamp_start']
        final_ts = kwargs['timestamp_end']
        return cls(token, initial_ts, final_ts)

    def __init__(
            self,
            token,
            ts_start,
            ts_end):

        self.token = token
        self.timestamp_start = ts_start
        self.timestamp_end = ts_end
        self.bsale_url = BSaleURL(ts_start, ts_end)

    def make_headers(self):
        return {'access_token': self.token}

    def bsale_request(self, **kwargs):
        url = self.bsale_url.make_url(**kwargs)
        logger.debug('Making Request to {}'.format(url))
        return makes_request(url, self.make_headers())

    def get_documents(self, expanded_fields=()):
        generic_args = {
            'expanded_fields': expanded_fields,
            'limit': BSaleURL.DEFAULT_LIMIT_VALUE,
        }
        specific_args = {
            'api_path': BSaleURL.DOCUMENTS_API,
        }
        more_data = True
        while more_data:
            data = self.bsale_request(**{**generic_args, **specific_args})
            yield data
            if 'next' not in data:
                more_data = False
            else:
                specific_args = {
                    'url': data['next'],
                }

    def get_stocks(self, expanded_fields=(), limit=0):
        generic_args = {
            'expanded_fields': expanded_fields,
            'limit': limit,
        }
        specific_args = {
            'api_path': BSaleURL.STOCKS_API,
        }
        more_data = True
        while more_data:
            data = self.bsale_request(**{**generic_args, **specific_args})
            yield data
            if 'next' not in data:
                more_data = False
            else:
                specific_args = {
                    'url': data['next'],
                }

    def get_payments(self, expanded_fields=(), limit=0):
        generic_args = {
            'expanded_fields': expanded_fields,
            'limit': limit,
        }
        specific_args = {
            'api_path': BSaleURL.PAYMENTS_API,
        }
        more_data = True
        while more_data:
            data = self.bsale_request(**{**generic_args, **specific_args})
            yield data
            if 'next' not in data:
                more_data = False
            else:
                specific_args = {
                    'url': data['next'],
                }

    def get_variants(self, expanded_fields=(), limit=0):
        generic_args = {
            'expanded_fields': expanded_fields,
            'limit': limit,
        }
        specific_args = {
            'api_path': BSaleURL.VARIANTS_API,
        }
        more_data = True
        while more_data:
            data = self.bsale_request(**{**generic_args, **specific_args})
            yield data
            if 'next' not in data:
                more_data = False
            else:
                specific_args = {
                    'url': data['next'],
                }

    def get_prices(self, price_list_id, expanded_fields=(), limit=0):
        generic_args = {
            'expanded_fields': expanded_fields,
            'limit': limit,
        }
        specific_args = {
            'api_path': BSaleURL.PRICES_API.format(price_list_id),
        }
        more_data = True
        while more_data:
            data = self.bsale_request(**{**generic_args, **specific_args})
            yield data
            if 'next' not in data:
                more_data = False
            else:
                specific_args = {
                    'url': data['next'],
                }
