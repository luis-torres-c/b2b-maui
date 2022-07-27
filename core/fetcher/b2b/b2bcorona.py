import os

from core.fetcher import DailyFetcher
from core.fetcher.b2b.base import B2BFile
from core.fetcher.b2b.base import B2BPortalBase
from core.fetcher.b2b.base import B2BWebOC
from core.fetcher.base import B2BWebFetcher
from core.sources.b2b.b2bcorona import CoronaB2BFileSource
from core.sources.b2b.b2bcorona import CoronaB2BOCWebSource
from core.sources.b2b.b2bcorona import CoronaB2BPortalSource
from core.sources.b2b.b2bcorona import CoronaB2BStockSource
from core.storages.onlycsv import OnlyCsvStorage
from core.storages.simple import SimpleDailyCsvStorage
from core.utils import create_id
from core.utils import datetime_to_wivo_format


class CoronaB2BOC(B2BWebOC, CoronaB2BOCWebSource, SimpleDailyCsvStorage, DailyFetcher):

    name = 'b2b-corona-oc'

    string_variables = {
        'username': 'B2B_USERNAME_CORONA',
        'password': 'B2B_PASSWORD_CORONA',
    }

    PORTAL = 'Corona'

    BASE_COLUMN_NAMES = [
        'Cliente',
        'Número de OC',
        'SKU',
        'Cod Sucursal de Destino',
        'EMPRESA',
        'Solicitado',
        'Costo Neto Unitario',
        'Fecha Vto',
        'Descripción',
        'Precio Normal',
        'Código Barra',
        'Cod Departamento',
        'Departamento',
        'DV',
        'Talla',
        'Cod Talla',
        'Estilo',
        'Desc Color',
        'Temporada',
        'Color',
        'Cod Producto Cliente',
        'Fecha Emisión'
    ]
    PARSE_COLUMN_NAMES = {
        'Núm. Orden': 'Número de OC',
        'Estilo Proveedor': ['SKU', 'Estilo'],
        'Cod. Lugar Destino': 'Cod Sucursal de Destino',
        'Cantidad': 'Solicitado',
        'Precio Costo Unitario (Neto)': 'Costo Neto Unitario',
        'Vencimiento': 'Fecha Vto',
        'Descripción Larga': 'Descripción',
        'Precio Lista': 'Precio Normal',
        'Cod. EAN': 'Código Barra',
        'Desc. Talla': ['Cod Departamento', 'Talla'],
        'DV': 'DV',
        'Cod. Talla': 'Cod Talla',
        'Desc. Color': 'Desc Color',
        'Rbr': 'Temporada',
        'Cod. Corona': 'Cod Producto Cliente',
        'Sbr': 'Color'
    }

    COLUMNS_WHIT_DATE = {
        'Fecha Vto': '%d/%m/%Y',
    }


class CoronaB2BFiles(B2BFile, CoronaB2BFileSource, OnlyCsvStorage, DailyFetcher):

    name = 'b2b-corona-files'

    PORTAL = 'Corona'

    IVA = 1.19

    PRODUCT_SUFFIX = ''
    STORE_NAME_PREFIX = ''

    string_variables = {
        'username': 'B2B_USERNAME_CORONA',
        'empresa': 'B2B_EMPRESA_CORONA',
        'repository_path': 'SOURCE_INT_PATH',
    }

    date_format = '%Y-%m-%d 00:00:00 00:00'

    @classmethod
    def settings(cls):
        args = super().settings()
        repository_path = os.environ.get(cls.string_variables['repository_path'], '')
        empresa = os.environ.get(cls.string_variables['empresa'], '')
        args['repository_path'] = repository_path
        args['b2b_empresa'] = empresa
        return args

    def base_data(self, row, date_time):
        b2bproduct_id = row['COD_CORONA']
        product_name = row['DESCRIPCION']
        store_id = row['COD_LOCAL']
        store_name_prefix = '{} '.format(self.STORE_NAME_PREFIX) if self.STORE_NAME_PREFIX else ''
        store_name = store_name_prefix + row['DESCRIPCION_LOCAL']
        self.chain_id = 'corona'
        chain_name = 'Corona'
        model = row['COD_PRODUCTO_PROVEEDOR']
        category_name = row['SUBRUBRO']
        brand = row['MARCA']

        base_data = {
            self.mapping_column_name('source_id'): create_id(self.chain_id),
            self.mapping_column_name('source_name'): chain_name,
            self.mapping_column_name('store_id'): create_id(self.chain_id + store_id),
            self.mapping_column_name('store_name'): store_name,
            self.mapping_column_name('product_id'): create_id(self.chain_id + self.PRODUCT_SUFFIX + b2bproduct_id),
            self.mapping_column_name('product_name'): product_name,
            self.mapping_column_name('brand_id'): create_id(self.chain_id + brand),
            self.mapping_column_name('brand_name'): brand,
            self.mapping_column_name('category_id'): create_id(self.chain_id + category_name),
            self.mapping_column_name('category_name'): category_name,
            self.mapping_column_name('codstore_id'): create_id(self.chain_id + store_id),
            self.mapping_column_name('codstore_name'): store_id,
            self.mapping_column_name('codproduct_id'): create_id(self.chain_id + b2bproduct_id),
            self.mapping_column_name('codproduct_name'): b2bproduct_id,
            self.mapping_column_name('codmodel_id'): create_id(self.chain_id + model),
            self.mapping_column_name('codmodel_name'): model,
            'datetime': datetime_to_wivo_format(date_time)}

        return base_data

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):
        net_values = kwargs['net_values']
        allow_sales_zero = bool(kwargs.get('allow_sales_zero', True))

        sale = row['VTA_PUBLICO($)'] / self.IVA if net_values else row['VTA_PUBLICO($)']

        saleunits_value = {
            'value': row['VTA_PERIODO(u)']
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


class CoronaB2BStock(CoronaB2BStockSource, CoronaB2BFiles):

    name = 'b2b-corona-stock-files'

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):

        if 'INVENTARIO(u)' in row:
            net_values = kwargs['net_values']

            stockunits_value = {
                'value': row['INVENTARIO(u)']
            }

            stock = row['INVENTARIO($)'] if net_values else row['INVENTARIO($)'] * self.IVA

            stocks_value = {
                'value': stock
            }

            self.append(metrics['stockunit'], date_time, {**base_data, **stockunits_value})
            self.append(metrics['stock'], date_time, {**base_data, **stocks_value})


class CoronaB2B(
        B2BPortalBase,
        CoronaB2BPortalSource,
        B2BWebFetcher):

    name = 'b2b-corona-portal'

    related_fetchers = [CoronaB2BFiles]

    string_variables = {
        'username': 'B2B_USERNAME_CORONA',
        'password': 'B2B_PASSWORD_CORONA',
        'empresa': 'B2B_EMPRESA_CORONA',
    }

    PORTAL = 'Corona'

    @classmethod
    def settings(cls):
        args = super().settings()
        empresa = os.environ.get(cls.string_variables['empresa'], '')
        args['b2b_empresa'] = empresa

        return args
