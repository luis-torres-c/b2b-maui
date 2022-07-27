import os

from core.fetcher import DailyFetcher
from core.fetcher.b2b.base import B2BFile
from core.fetcher.b2b.base import B2BPortalBase
from core.fetcher.base import B2BWebFetcher
from core.sources.b2b.b2babcdin import AbcdinB2BFileSource
from core.sources.b2b.b2babcdin import AbcdinB2BPortalSource
from core.sources.b2b.b2babcdin import AbcdinB2BStockSource
from core.utils import datetime_to_wivo_format
from core.storages.onlycsv import OnlyCsvStorage


class AbcdinB2BSales(B2BFile, AbcdinB2BFileSource, OnlyCsvStorage, DailyFetcher):

    name = 'b2b-abcdin-files'

    date_format = '%Y-%m-%d 00:00:00 00:00'
    IVA = 1.19
    PRODUCT_SUFFIX = ''

    string_variables = {
        'username': 'B2B_USERNAME_ABCDIN',
        'repository_path': 'SOURCE_INT_PATH'
    }

    PORTAL = 'Abcdin'

    @classmethod
    def settings(cls):
        args = super().settings()
        repository_path = os.environ.get(cls.string_variables['repository_path'], '')
        args['repository_path'] = repository_path
        args['b2b_empresa'] = ''
        return args

    def base_data(self, row, date_time):
        b2bproduct_id = row['COD_ABCDIN']
        product_name = row['DESCRIPCION_PRODUCTO']
        store_id = row['COD_LOCAL']
        store_name = row['DESCRIPCION_LOCAL']
        self.chain_id = 'abcdin'
        chain_name = 'Abcdin'
        codmodel = row['COD_PROVEEDOR']
        brand_name = row['MARCA']

        base_data = {
            self.mapping_column_name('source_id'): self.chain_id,
            self.mapping_column_name('source_name'): chain_name,
            self.mapping_column_name('store_id'): self.chain_id + store_id,
            self.mapping_column_name('store_name'): store_name,
            self.mapping_column_name('product_id'): self.chain_id + self.PRODUCT_SUFFIX + b2bproduct_id,
            self.mapping_column_name('product_name'): product_name,
            self.mapping_column_name('brand_id'): self.chain_id + brand_name,
            self.mapping_column_name('brand_name'): brand_name,
            self.mapping_column_name('category_id'): '',
            self.mapping_column_name('category_name'): '',
            self.mapping_column_name('codstore_id'): self.chain_id + store_id,
            self.mapping_column_name('codstore_name'): store_id,
            self.mapping_column_name('codproduct_id'): self.chain_id + b2bproduct_id,
            self.mapping_column_name('codproduct_name'): b2bproduct_id,
            self.mapping_column_name('codmodel_id'): self.chain_id + codmodel,
            self.mapping_column_name('codmodel_name'): codmodel,
            'datetime': datetime_to_wivo_format(date_time)
        }

        return base_data

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):

        net_values = kwargs['net_values']
        allow_sales_zero = bool(kwargs.get('allow_sales_zero', True))

        saleunits_value = {
            'value': row['VTA_PERIODO(u)']
        }

        sales = row['VTA_PERIODO($)']
        if not net_values:
            sales = row['VTA_PERIODO($)'] / self.IVA

        sales_value = {
            'value': sales
        }

        if allow_sales_zero:
            self.append(metrics['salesunit'], date_time, {**base_data, **saleunits_value})
            self.append(metrics['sale'], date_time, {**base_data, **sales_value})
        else:
            if saleunits_value['value'] != 0:
                self.append(metrics['salesunit'], date_time, {**base_data, **saleunits_value})
            if sales_value['value'] != 0:
                self.append(metrics['sale'], date_time, {**base_data, **sales_value})


class AbcdinB2BStock(AbcdinB2BStockSource, AbcdinB2BSales):

    name = 'b2b-abcdin-stock-files'

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):

        if 'INVENTARIO(u)' in row:

            net_values = kwargs['net_values']

            stockunits_value = {
                'value': row['INVENTARIO(u)']
            }

            stock = row['INVENTARIO($)']
            if not net_values:
                stock = row['INVENTARIO($)'] / self.IVA

            stocks_value = {
                'value': stock
            }
            self.append(metrics['stockunit'], date_time, {**base_data, **stockunits_value})
            self.append(metrics['stock'], date_time, {**base_data, **stocks_value})


class AbcdinB2B(
        B2BPortalBase,
        AbcdinB2BPortalSource,
        B2BWebFetcher):

    name = 'b2b-abcdin-portal'

    related_fetchers = [AbcdinB2BSales]

    string_variables = {
        'username': 'B2B_USERNAME_ABCDIN',
        'password': 'B2B_PASSWORD_ABCDIN',
        'empresa': 'B2B_EMPRESA_ABCDIN'
    }

    PORTAL = 'Abcdin'
