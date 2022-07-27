import os

from core.fetcher import DailyFetcher
from core.sources.b2b.b2bsalco import SalcoB2BPortalSource, SalcoB2BSalesSource, SalcoB2BStockSource
from core.storages.onlycsv import OnlyCsvStorage
from core.utils import datetime_to_wivo_format, create_id
from core.fetcher.b2b.base import B2BPortalBase, B2BFile
from core.fetcher.base import B2BWebFetcher


class SalcoB2BSales(B2BFile, SalcoB2BSalesSource, OnlyCsvStorage, DailyFetcher):

    name = 'b2b-salco-files'

    string_variables = {
        'username': 'B2B_USERNAME_SALCO',
        'repository_path': 'SOURCE_INT_PATH',
    }

    date_format = '%Y-%m-%d 00:00:00 00:00'

    @classmethod
    def settings(cls):
        variables = super().settings()
        repository_path = os.environ.get(cls.string_variables['repository_path'], '')
        variables['repository_path'] = repository_path
        return variables

    def base_data(self, row, date_time):

        self.chain_id = 'salco'
        chain_name = 'Salco'
        product_id = self.chain_id + create_id(row['Sku'])
        product_name = row['Descripción Producto']
        cod_sucursal = row['Id Sucursal']
        store_id = self.chain_id + cod_sucursal
        store_name = row['Descripción']
        brand_id = self.chain_id
        brand_name = ''
        category_id = self.chain_id + create_id(row['category_id'])
        category_name = row['category_name']
        codmodel = ''
        codmodel_id = self.chain_id

        base_data = {
            self.mapping_column_name('brand_id'): brand_id,
            self.mapping_column_name('brand_name'): brand_name,
            self.mapping_column_name('source_id'): self.chain_id,
            self.mapping_column_name('source_name'): chain_name,
            self.mapping_column_name('store_id'): store_id,
            self.mapping_column_name('store_name'): store_name,
            self.mapping_column_name('product_id'): product_id,
            self.mapping_column_name('product_name'): product_name,
            self.mapping_column_name('category_id'): category_id,
            self.mapping_column_name('category_name'): category_name,
            self.mapping_column_name('codstore_id'): store_id,
            self.mapping_column_name('codstore_name'): cod_sucursal,
            self.mapping_column_name('codproduct_id'): product_id,
            self.mapping_column_name('codproduct_name'): row['Sku'],
            self.mapping_column_name('codmodel_id'): codmodel_id,
            self.mapping_column_name('codmodel_name'): codmodel,
            'datetime': datetime_to_wivo_format(date_time)}

        return base_data

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):

        net_values = kwargs['net_values']
        allow_sales_zero = bool(kwargs.get('allow_sales_zero', True))

        saleunits_value = {
            'value': int(float(row['Unidades']))
        }

        sales = float(row['Venta'])
        if not net_values:
            sales = format(sales * 1.19, '.2f') if sales != 0 else 0

        sales_value = {
            'value': sales
        }

        if allow_sales_zero:
            self.append(
                metrics['salesunit'], date_time, {
                    **base_data, **saleunits_value})
            self.append(
                metrics['sale'], date_time, {
                    **base_data, **sales_value})
        else:
            if saleunits_value['value'] != 0:
                self.append(
                    metrics['salesunit'], date_time, {
                        **base_data, **saleunits_value})
            if sales_value['value'] != 0:
                self.append(
                    metrics['sale'], date_time, {
                        **base_data, **sales_value})


class SalcoB2BStock(SalcoB2BStockSource, SalcoB2BSales):

    name = 'b2b-salco-stock'

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):
        stockunits_value = {
            'value': row['STOCK Local']
        }

        self.append(metrics['stockunit'], date_time, {**base_data, **stockunits_value})


class SalcoB2BPortal(B2BPortalBase, SalcoB2BPortalSource, B2BWebFetcher):

    name = 'b2b-salco-portal'

    DAYS_TO_CONSOLIDATE = 44

    related_fetchers = [SalcoB2BSales]
