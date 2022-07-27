import datetime
import os

from core.fetcher import DailyFetcher
from core.sources.b2b.b2bpcfactory import PcFactoryB2BWebSource
from core.storages.onlycsv import OnlyCsvStorage
from core.fetcher.b2b.base import B2BWeb
from core.utils import datetime_to_wivo_format


class PcFactoryB2B(
        B2BWeb,
        PcFactoryB2BWebSource,
        OnlyCsvStorage,
        DailyFetcher):

    name = 'b2b-pcfactory-sales'
    date_format = '%d/%m/%Y'

    STORE_NAME_PREFIX = ''

    string_variables = {
        'username': 'B2B_USERNAME_PCFACTORY',
        'password': 'B2B_PASSWORD_PCFACTORY',
        '2captcha_key': '2CAPTCHA_KEY',
        'captcha_googlekey': 'CAPTCHA_GOOGLEKEY',
    }

    @classmethod
    def settings(cls):
        variables = super().settings()
        variables['captcha_key'] = os.environ.get('CAPTCHA_KEY', '')
        variables['captcha_googlekey'] = os.environ.get('CAPTCHA_GOOGLEKEY', '')
        return variables

    def parse_data(self, connector, **kwargs):
        b2bsaleunits = []
        b2bstockunits = []
        b2bproducts = []
        stores = []

        date_in_datetime = self.actual_date - datetime.timedelta(days=1)
        stock_datetime = datetime.datetime.today() - datetime.timedelta(days=1)
        date = datetime_to_wivo_format(date_in_datetime)

        # TODO: need to change to standar values. INT-337

        for row in connector.detalle_venta(self.actual_date):
            if row['type'] == 'stores':
                store_name_prefix = '{} '.format(self.STORE_NAME_PREFIX) if self.STORE_NAME_PREFIX else ''
                store = {
                    'b2bstore_id': row['store_id'],
                    'b2bstore_name': store_name_prefix + row['store_name'],
                }
                if store not in stores:
                    stores.append(store)
            elif row['type'] == 'b2bproduct':
                b2bproduct = {
                    'b2bproduct_id': row['b2bproduct_id'],
                    'b2bproduct_name': row['b2bproduct_name'],
                }
                if b2bproduct not in b2bproducts:
                    b2bproducts.append(b2bproduct)
            elif row['type'] == 'b2bstockunit':
                b2bstock = {
                    'datetime': date,
                    'value': row['value'],
                    'b2bproduct_id': row['b2bproduct_id'],
                    'b2bstore_id': row['store_id'],
                    'b2bchain_id': row['chain_id'],
                }
                if b2bstock not in b2bstockunits:
                    b2bstockunits.append(b2bstock)
            elif row['type'] == 'b2bsaleunits':
                b2bsalesunit = {
                    'datetime': date,
                    'value': row['value'],
                    'b2bproduct_id': row['b2bproduct_id'],
                    'b2bstore_id': row['store_id'],
                    'b2bchain_id': row['chain_id'],
                }
                if b2bsalesunit not in b2bsaleunits:
                    b2bsaleunits.append(b2bsalesunit)
        if stock_datetime.year != (
            self.actual_date -
            datetime.timedelta(
                days=1)).year or stock_datetime.month != (
            self.actual_date -
            datetime.timedelta(
                days=1)).month or stock_datetime.day != (
                    self.actual_date -
                    datetime.timedelta(
                        days=1)).day:
            b2bstockunits = []
        return [{'metric': 'b2bstockunit', 'date': stock_datetime, 'records': b2bstockunits},
                {'metric': 'b2bsalesunit', 'date': date_in_datetime, 'records': b2bsaleunits},
                {'object': 'b2bstore', 'date': date_in_datetime, 'records': stores},
                {'object': 'b2bproduct', 'date': date_in_datetime, 'records': b2bproducts},
                ]
