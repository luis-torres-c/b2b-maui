import datetime
import os
import time

from core.fetcher import DailyFetcher
from core.fetcher.b2b.base import B2BFile
from core.fetcher.b2b.base import B2BPortalBase
from core.fetcher.base import B2BWebFetcher
from core.sources.b2b.b2bunimarc import UnimarcB2BFileSource
from core.sources.b2b.b2bunimarc import UnimarcB2BPortalSource
from core.sources.b2b.b2bunimarc import UnimarcB2BStockSource
from core.storages.onlycsv import OnlyCsvStorage
from core.utils import create_id
from core.utils import datetime_to_wivo_format


class UnimarcB2BSales(B2BFile, UnimarcB2BFileSource, OnlyCsvStorage, DailyFetcher):

    name = 'b2b-unimarc-files'

    date_format = '%Y-%m-%d 00:00:00 00:00'

    PRODUCT_SUFFIX = ''
    STORE_NAME_PREFIX = ''

    IVA = 1.19

    PORTAL = 'Unimarc'

    string_variables = {
        'username': 'B2B_USERNAME_UNIMARC',
        'empresa': 'B2B_EMPRESA_UNIMARC',
        'repository_path': 'SOURCE_INT_PATH',
    }

    @classmethod
    def settings(cls):
        args = super().settings()
        repository_path = os.environ.get(cls.string_variables['repository_path'], '')
        empresa = os.environ.get(cls.string_variables['empresa'], '')
        args['repository_path'] = repository_path
        args['b2b_empresa'] = empresa
        return args

    def base_data(self, row, date_time):
        b2bproduct_id = row['Cód. Unimarc']
        product_name = row['Descripción Producto']
        store_id = row['Cód. Local']
        store_name = row['Descripción Local']
        store_name_prefix = '{} '.format(
            self.STORE_NAME_PREFIX) if self.STORE_NAME_PREFIX else ''
        store_name = store_name_prefix + store_name
        self.chain_id = 'unimarc'
        chain_name = 'unimarc'
        brand_id = self.chain_id + create_id(row['Marca'])
        brand_name = row['Marca']
        codmodel = row['Cód. Proveedor']
        category_name = ''

        base_data = {
            self.mapping_column_name('brand_id'): brand_id,
            self.mapping_column_name('brand_name'): brand_name,
            self.mapping_column_name('source_id'): self.chain_id,
            self.mapping_column_name('source_name'): chain_name,
            self.mapping_column_name('store_id'): self.chain_id + store_id,
            self.mapping_column_name('store_name'): store_name,
            self.mapping_column_name('product_id'): self.chain_id + self.PRODUCT_SUFFIX + b2bproduct_id,
            self.mapping_column_name('product_name'): product_name,
            self.mapping_column_name('category_id'): self.chain_id + category_name,
            self.mapping_column_name('category_name'): category_name,
            self.mapping_column_name('codstore_id'): self.chain_id + store_id,
            self.mapping_column_name('codstore_name'): store_id,
            self.mapping_column_name('codproduct_id'): self.chain_id + b2bproduct_id,
            self.mapping_column_name('codproduct_name'): b2bproduct_id,
            self.mapping_column_name('codmodel_id'): self.chain_id + codmodel,
            self.mapping_column_name('codmodel_name'): codmodel,
            'datetime': datetime_to_wivo_format(date_time)}

        return base_data

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):

        net_values = kwargs['net_values']
        allow_sales_zero = bool(kwargs.get('allow_sales_zero', True))

        sale_value = row['Vta. Púb.(s/IVA)']
        if not net_values:
            sale_value = row['Vta. Púb.(s/IVA)'] * self.IVA

        saleunit_value = row['Vta. Unid.']

        saleunits_value = {
            'value': saleunit_value,
        }
        sales_value = {
            'value': sale_value
        }

        if allow_sales_zero:
            self.append(metrics['salesunit'], date_time, {**base_data, **saleunits_value})
            self.append(metrics['sale'], date_time, {**base_data, **sales_value})
        else:
            if saleunit_value != 0 and sale_value != 0:
                self.append(metrics['salesunit'], date_time, {**base_data, **saleunits_value})
                self.append(metrics['sale'], date_time, {**base_data, **sales_value})


class UnimarcB2BStock(UnimarcB2BStockSource, UnimarcB2BSales):

    name = 'b2b-unimarc-stock-files'

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):

        if 'Inventario(u)' in row:
            net_values = kwargs['net_values']
            stockunits_value = {
                'value': row['Inventario(u)']
            }
            stock = row['Inv. a Costo(s/IVA)']
            if not net_values:
                stock = row['Inv. a Costo(s/IVA)'] * self.IVA

            stocks_value = {
                'value': stock
            }
            self.append(metrics['stockunit'], date_time, {**base_data, **stockunits_value})
            self.append(metrics['stock'], date_time, {**base_data, **stocks_value})


class UnimarcB2BWeb(
        B2BPortalBase,
        UnimarcB2BPortalSource,
        B2BWebFetcher):

    name = 'b2b-unimarc-portal'

    PORTAL = 'Unimarc'

    string_variables = {
        'username': 'B2B_USERNAME_UNIMARC',
        'password': 'B2B_PASSWORD_UNIMARC',
        'empresa': 'B2B_EMPRESA_UNIMARC',
    }

    related_fetchers = [UnimarcB2BSales]

    @classmethod
    def settings(cls):
        variables = super().settings()
        empresa = os.environ.get(cls.string_variables['empresa'], '')
        variables['b2b_empresa'] = empresa
        return variables

    def parse_data(self, connector, **kwargs):
        date_format = '%Y-%m-%d'
        date_from = datetime.datetime.strptime(kwargs['from'], date_format).date()
        date_to = datetime.datetime.strptime(kwargs['to'], date_format).date()
        result = []
        while date_from <= date_to:
            new_period = {
                'from': date_from.strftime(date_format),
                'to': (date_from + datetime.timedelta(days=10)).strftime(date_format) if date_from + datetime.timedelta(days=15) < date_to else date_to.strftime(date_format)
            }
            kwargs.update(new_period)
            result += connector.generate_files(**kwargs)
            date_from += datetime.timedelta(days=10)
            if date_from <= date_to:
                time.sleep(60)

        return result
