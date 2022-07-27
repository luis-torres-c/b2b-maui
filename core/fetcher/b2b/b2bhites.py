import os

from core.fetcher import DailyFetcher
from core.fetcher.b2b.base import B2BFile
from core.fetcher.b2b.base import B2BPortalBase
from core.fetcher.b2b.base import B2BWebOC
from core.fetcher.base import B2BWebFetcher
from core.sources.b2b.b2bhites import HitesB2BFileSource
from core.sources.b2b.b2bhites import HitesB2BPortalSource
from core.sources.b2b.b2bhites import HitesB2BStockSource
from core.sources.b2b.b2bhites import HitesB2BOCWebSource
from core.storages.replacing import ReplacingLogicCSVStorage
from core.storages.simple import SimpleDailyCsvStorage
from core.utils import create_id
from core.utils import datetime_to_wivo_format


class HitesB2BOC(B2BWebOC, HitesB2BOCWebSource, SimpleDailyCsvStorage, DailyFetcher):

    name = 'b2b-hites-oc'

    string_variables = {
        'username': 'B2B_USERNAME_HITES',
        'password': 'B2B_PASSWORD_HITES',
        'repository_path': 'SOURCE_INT_PATH'
    }

    PORTAL = 'Hites'

    BASE_COLUMN_NAMES = ['Cliente', 'Número de OC', 'SKU', 'Cod Sucursal de Destino', 'EMPRESA', 'Solicitado', 'Costo Neto Unitario', 'Fecha Vto', 'Descripción', 'Precio Normal', 'Código Barra', 'Cod Departamento', 'Departamento', 'DV', 'Talla', 'Cod Talla', 'Estilo', 'Desc Color', 'Temporada', 'Color', 'Cod Producto Cliente', 'Fecha Emisión']
    PARSE_COLUMN_NAMES = {
        'Número de Orden': 'Número de OC',
        'Estilo': ['SKU', 'Estilo'],
        'Cód. Suc. Destino': 'Cod Sucursal de Destino',
        'Unidades Solicitadas': 'Solicitado',
        'Costo Neto': 'Costo Neto Unitario',
        'Fecha Entrega': 'Fecha Vto',
        'Descripción Art.': 'Descripción',
        'Precio': 'Precio Normal',
        'Código Barra': 'Código Barra',
        'Departamento': 'Departamento',
        'Dig. Verif.': 'DV',
        'Talla': 'Talla',
        'Temporada': 'Temporada',
        'Color': 'Color',
        'Código Hites': 'Cod Producto Cliente',
        'Fecha Emisión': 'Fecha Emisión',
    }

    COLUMNS_WHIT_DATE = {
        'Fecha Vto': '%d-%m-%Y',
        'Fecha Emisión': '%d-%m-%Y',
    }

    @classmethod
    def settings(cls):
        args = super().settings()
        repository_path = os.environ.get(cls.string_variables['repository_path'], '')
        args['repository_path'] = repository_path
        return args


class HitesB2BCustomSales(
        B2BWebOC,
        HitesB2BFileSource,
        SimpleDailyCsvStorage,
        DailyFetcher):

    name = 'b2b-hites-custom-sales'

    DATA_NAME = 'sales'

    PORTAL = 'Hites'

    BASE_COLUMN_NAMES = [
        'Cod Tienda',
        'Tienda',
        'Cod Producto',
        'Producto',
        'Categoria',
        'Marca',
        'Cod Modelo',
        'Venta',
        'Unidades Vendidas',
        'Stock Valorizado',
        'Unidades de Stock',
    ]

    PARSE_COLUMN_NAMES = {
        'COD_LOCAL': 'Cod Tienda',
        'DESCRIPCION_LOCAL': 'Tienda',
        'COD_HITES': 'Cod Producto',
        'DESCRIPCION_PRODUCTO': 'Producto',
        'MARCA': 'Marca',
        'COD_PROVEEDOR': 'Cod Modelo',
        'VTA_PUBLICO($)': 'Venta',
        'VTA_PERIODO(u)': 'Unidades Vendidas',
        'INVENTARIO($)': 'Stock Valorizado',
        'INVENTARIO(u)': 'Unidades de Stock'
    }

    COLUMNS_WHIT_DATE = {}

    string_variables = {
        'username': 'B2B_USERNAME_HITES',
        'password': 'B2B_PASSWORD_HITES',
        'repository_path': 'SOURCE_INT_PATH',
    }

    @classmethod
    def settings(cls):
        args = super().settings()
        repository_path = os.environ.get(cls.string_variables['repository_path'], '')
        args['repository_path'] = repository_path
        args['b2b_empresa'] = ''
        return args


class HitesB2BSales(B2BFile, HitesB2BFileSource, ReplacingLogicCSVStorage, DailyFetcher):

    name = 'b2b-hites-files'

    date_format = '%Y-%m-%d 00:00:00 00:00'
    STORE_NAME_PREFIX = ''
    PRODUCT_SUFFIX = ''
    IVA = 1.19
    PORTAL = 'Hites'

    string_variables = {
        'username': 'B2B_USERNAME_HITES',
        'repository_path': 'SOURCE_INT_PATH',
    }

    @classmethod
    def settings(cls):
        args = super().settings()
        repository_path = os.environ.get(cls.string_variables['repository_path'], '')
        args['repository_path'] = repository_path
        args['b2b_empresa'] = ''
        return args

    def base_data(self, row, date_time):
        b2bproduct_id = row['COD_HITES']
        product_name = row['DESCRIPCION_PRODUCTO']
        store_id = row['COD_LOCAL']
        store_name_prefix = '{} '.format(self.STORE_NAME_PREFIX) if self.STORE_NAME_PREFIX else ''
        store_name = store_name_prefix + row['DESCRIPCION_LOCAL']
        self.chain_id = 'hites'
        chain_name = 'Hites'
        brand_id = self.chain_id + create_id(row['MARCA'])
        brand_name = row['MARCA']
        codmodel = row['COD_PROVEEDOR']

        base_data = {
            self.mapping_column_name('brand_id'): brand_id,
            self.mapping_column_name('brand_name'): brand_name,
            self.mapping_column_name('source_id'): self.chain_id,
            self.mapping_column_name('source_name'): chain_name,
            self.mapping_column_name('store_id'): self.chain_id + store_id,
            self.mapping_column_name('store_name'): store_name,
            self.mapping_column_name('product_id'): self.chain_id + self.PRODUCT_SUFFIX + b2bproduct_id,
            self.mapping_column_name('product_name'): product_name,
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

        sale = row['VTA_PUBLICO($)']
        if not net_values:
            sale = row['VTA_PUBLICO($)'] * 1.19

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


class HitesB2BStock(HitesB2BStockSource, HitesB2BSales):

    name = 'b2b-hites-stock-files'

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):

        if 'INVENTARIO(u)' in row:
            net_values = kwargs['net_values']

            stockunits_value = {
                'value': row['INVENTARIO(u)']
            }
            stock = row['INVENTARIO($)']
            if not net_values:
                stock = row['INVENTARIO($)'] * 1.19
            stocks_value = {
                'value': stock
            }
            self.append(metrics['stockunit'], date_time, {**base_data, **stockunits_value})
            self.append(metrics['stock'], date_time, {**base_data, **stocks_value})


class HitesB2B(
        B2BPortalBase,
        HitesB2BPortalSource,
        B2BWebFetcher):

    name = 'b2b-hites-portal'

    string_variables = {
        'username': 'B2B_USERNAME_HITES',
        'password': 'B2B_PASSWORD_HITES'
    }

    PORTAL = 'Hites'

    related_fetchers = [HitesB2BSales]
