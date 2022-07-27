import os

from core.fetcher import DailyFetcher
from core.sources.b2b.b2bmaicao import MaicaoB2BPortalSource, MaicaoB2BFileSource
from core.utils import datetime_to_wivo_format, create_id
from core.fetcher.b2b.base import B2BPortalBase, B2BFile
from core.fetcher.base import B2BWebFetcher
from core.storages.onlycsv import OnlyCsvStorage


class MaicaoB2B(
        B2BPortalBase,
        MaicaoB2BPortalSource,
        B2BWebFetcher):

    name = 'b2b-maicao-portal'

    string_variables = {
        'username': 'B2B_USERNAME_MAICAO',
        'password': 'B2B_PASSWORD_MAICAO',
        'empresa': 'B2B_EMPRESA_MAICAO',
    }


class MaicaoB2BSales(B2BFile, MaicaoB2BFileSource, OnlyCsvStorage, DailyFetcher):

    name = 'b2b-maicao-files'

    string_variables = {
        'username': 'B2B_USERNAME_MAICAO',
        'empresa': 'B2B_EMPRESA_MAICAO',
        'repository_path': 'SOURCE_INT_PATH',
    }

    date_format = '%Y-%m-%d %H:%M:%S 00:00'

    IVA = 1.19

    @classmethod
    def settings(cls):
        args = super().settings()
        repository_path = os.environ.get(cls.string_variables['repository_path'], '')
        args['repository_path'] = repository_path
        return args

    def base_data(self, row, date_time):

        self.chain_id = 'maicao'
        chain_name = 'Maicao'
        product_id = self.chain_id + create_id(row['Articulo'])
        product_name = row['Descripcion']
        cod_sucursal = row['Cod. Sucursal']
        store_id = self.chain_id + cod_sucursal
        store_name = row['Sucursal']
        brand_id = self.chain_id + create_id(row['Marca'])
        brand_name = row['Marca']
        category_id = self.chain_id + create_id(row['Categoria'])
        category_name = row['Categoria']
        codmodel = row['Ean']
        codmodel_id = self.chain_id + codmodel

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
            self.mapping_column_name('codproduct_name'): row['Articulo'],
            self.mapping_column_name('codmodel_id'): codmodel_id,
            self.mapping_column_name('codmodel_name'): codmodel,
            'datetime': datetime_to_wivo_format(date_time)}

        return base_data

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):

        net_values = kwargs['net_values']
        allow_sales_zero = bool(kwargs.get('allow_sales_zero', True))

        saleunits_value = {
            'value': row['Cantidad']
        }

        sales = row['Vta.Neta $']
        if not net_values:
            sales = sales * self.IVA

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


class MaicaoB2BStock(MaicaoB2BSales):

    name = 'b2b-maicao-stock-files'

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):

        stockunits_value = {
            'value': row['Saldo local']
        }

        self.append(metrics['stockunit'], date_time, {**base_data, **stockunits_value})
