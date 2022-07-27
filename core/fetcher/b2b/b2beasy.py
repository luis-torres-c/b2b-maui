import os

from core.fetcher import DailyFetcher
from core.fetcher.b2b.base import B2BFile
from core.fetcher.b2b.base import B2BPortalBase
from core.fetcher.base import B2BWebFetcher
from core.sources.b2b.b2beasy import EasyB2BFileSource
from core.sources.b2b.b2beasy import EasyB2BPortalSource
from core.sources.b2b.b2beasy import EasyB2BStockSource
from core.storages.onlycsv import OnlyCsvStorage
from core.utils import datetime_to_wivo_format


class EasyB2BFiles(B2BFile, EasyB2BFileSource, OnlyCsvStorage, DailyFetcher):

    name = 'b2b-easy-files'

    date_format = '%Y-%m-%d 00:00:00 00:00'
    PORTAL = 'Easy'

    IVA = 1.19
    PRODUCT_SUFFIX = ''

    string_variables = {
        'username': 'B2B_USERNAME_EASY',
        'empresa': 'B2B_EMPRESA_EASY',
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
        b2bproduct_id = row['Cód. Cencosud']
        product_name = row['Descripción']
        store_id = row['Cód. Local']
        store_name = row['Descripción Local']
        self.chain_id = 'easy'
        chain_name = 'Easy'
        category_name = row['Categoría Producto Nivel 4']
        codmodel = row['Cod. Proveedor']
        brand_name = row.get('Marca', 'sinmarca')

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

        sale = row['Venta  Púb.($)']
        if not net_values:
            sale = float(row['Venta  Púb.($)']) * self.IVA

        saleunits_value = {
            'value': row['Venta Período(u)']
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


class EasyB2BStock(EasyB2BStockSource, EasyB2BFiles):

    name = 'b2b-easy-stock-files'

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):

        if 'Inv.(u)' in row:
            net_values = kwargs['net_values']
            stockunits_value = {
                'value': row['Inv.(u)']
            }
            stock = row['Inv. ($)']
            if not net_values:
                stock = row['Inv. ($)'] * self.IVA
            stocks_value = {
                'value': stock
            }
            self.append(metrics['stockunit'], date_time, {**base_data, **stockunits_value})
            self.append(metrics['stock'], date_time, {**base_data, **stocks_value})


class EasyB2B(
        B2BPortalBase,
        EasyB2BPortalSource,
        B2BWebFetcher):

    name = 'b2b-easy-portal'

    string_variables = {
        'username': 'B2B_USERNAME_EASY',
        'password': 'B2B_PASSWORD_EASY',
        'empresa': 'B2B_EMPRESA_EASY'
    }

    PORTAL = 'Easy'

    related_fetchers = [EasyB2BFiles]

    @classmethod
    def settings(cls):
        args = super().settings()
        empresa = os.environ.get(cls.string_variables['empresa'], '')
        args['b2b_empresa'] = empresa

        return args
