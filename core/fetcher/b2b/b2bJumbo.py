import os

from core.fetcher import DailyFetcher
from core.fetcher.b2b.base import B2BFile
from core.fetcher.b2b.base import B2BPortalBase
from core.fetcher.base import B2BWebFetcher
from core.sources.b2b.b2bjumbo import NewJumboB2BFileSource
from core.sources.b2b.b2bjumbo import NewJumboB2BPortalSource
from core.sources.b2b.b2bjumbo import NewJumboB2BStockSource
from core.storages.onlycsv import OnlyCsvStorage
from core.utils import datetime_to_wivo_format


class NewJumboB2BFiles(B2BFile, NewJumboB2BFileSource, OnlyCsvStorage, DailyFetcher):

    name = 'b2b-jumbo-files'

    date_format = '%Y-%m-%d 00:00:00 00:00'
    PORTAL = 'Jumbo'

    IVA = 1.19
    ALLOWED_CHAIN_IDS = [
        'jumbo',
        'santaisabel',
    ]
    PRODUCT_SUFFIX = ''

    string_variables = {
        'username': 'B2B_USERNAME_JUMBO',
        'empresa': 'B2B_EMPRESA_JUMBO',
        'repository_path': 'SOURCE_INT_PATH'
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
        b2bproduct_id = row['COD_CENCOSUD']
        product_name = row['DESCRIPCION_PRODUCTO']
        store_id = row['COD_LOCAL']
        store_name = row['DESCRIPCION_LOCAL']
        self.chain_id = 'jumbo' if store_id[0] == 'J' else 'santaisabel'
        chain_name = 'Jumbo' if store_id[0] == 'J' else 'Santa Isabel'
        category_name = row['GRUPO']
        codmodel = row['COD_PROVEEDOR']
        brand_name = row['MARCA']

        # Mechanism to filter chains in others cases.
        if self.chain_id not in self.ALLOWED_CHAIN_IDS:
            return {}

        base_data = {
            self.mapping_column_name('source_id'): self.chain_id,
            self.mapping_column_name('source_name'): chain_name,
            self.mapping_column_name('store_id'): self.chain_id + store_id,
            self.mapping_column_name('store_name'): store_name,
            self.mapping_column_name('product_id'): self.chain_id + self.PRODUCT_SUFFIX + b2bproduct_id,
            self.mapping_column_name('product_name'): product_name,
            self.mapping_column_name('brand_id'): self.chain_id + brand_name,
            self.mapping_column_name('brand_name'): brand_name,
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

        sale = row['VTA_PUBLICO($)']
        if not net_values:
            sale = row['VTA_PUBLICO($)'] * self.IVA

        saleunits_value = {
            'value': row['VENTA(Un)']
        }
        sales_value = {
            'value': sale
        }

        if allow_sales_zero:
            self.append(metrics['salesunit'], date_time, {**base_data, **saleunits_value})
            self.append(metrics['sale'], date_time, {**base_data, **sales_value})
        else:
            if saleunits_value['value'] != 0:
                self.append(metrics['salesunit'], date_time, {**base_data, **saleunits_value})
            if sales_value['value'] != 0:
                self.append(metrics['sale'], date_time, {**base_data, **sales_value})


class NewJumboB2BStock(NewJumboB2BStockSource, NewJumboB2BFiles):

    name = 'b2b-jumbo-stock-files'

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):

        if 'INV_ACTUAL(Un)' in row:
            net_values = kwargs['net_values']
            stockunits_value = {
                'value': row['INV_ACTUAL(Un)']
            }
            stock = row['INV_ACTUAL_COSTO($)']
            if not net_values:
                stock = row['INV_ACTUAL_COSTO($)'] * self.IVA
            stocks_value = {
                'value': stock
            }
            self.append(metrics['stockunit'], date_time, {**base_data, **stockunits_value})
            self.append(metrics['stock'], date_time, {**base_data, **stocks_value})


class NewJumboB2B(
        B2BPortalBase,
        NewJumboB2BPortalSource,
        B2BWebFetcher):

    name = 'b2b-jumbo-portal'

    string_variables = {
        'username': 'B2B_USERNAME_JUMBO',
        'password': 'B2B_PASSWORD_JUMBO',
        'empresa': 'B2B_EMPRESA_JUMBO'
    }

    PORTAL = 'Jumbo'

    related_fetchers = [NewJumboB2BFiles]

    @classmethod
    def settings(cls):
        args = super().settings()
        empresa = os.environ.get(cls.string_variables['empresa'], '')
        args['b2b_empresa'] = empresa

        return args
