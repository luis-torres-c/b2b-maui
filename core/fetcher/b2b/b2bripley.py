import os

from core.fetcher import DailyFetcher
from core.fetcher.base import B2BWebFetcher
from core.sources.b2b.b2bripley import RipleyB2BPortalSource, RipleyB2BFileSource, RipleyB2BStockSource, \
    RipleyB2BOCWebSource, RipleyB2BPeruPortalSource, RipleyB2BOCTestWebSource
from core.utils import datetime_to_wivo_format, create_id
from core.fetcher.b2b.base import B2BWebOC
from core.fetcher.b2b.base import B2BPortalBase, B2BFile
from core.storages.simple import SimpleDailyCsvStorage
from core.storages.onlycsv import OnlyCsvStorage


class RipleyB2BOC(
        B2BWebOC,
        RipleyB2BOCWebSource,
        SimpleDailyCsvStorage,
        DailyFetcher):

    name = 'b2b-ripley-oc'

    string_variables = {
        'username': 'B2B_USERNAME_RIPLEY',
        'password': 'B2B_PASSWORD_RIPLEY',
        'empresa': 'B2B_EMPRESA_RIPLEY',
    }

    PORTAL = 'Ripley'

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
        'Número de OC': 'Número de OC',
        'Cód. Art. Prov.   (Case Pack)': 'SKU',
        'Cod. Sucursal de Destino': 'Cod Sucursal de Destino',
        'Solicitado (u)': 'Solicitado',
        'Costo Neto Unitario': 'Costo Neto Unitario',
        'Fecha Cancelacion': 'Fecha Vto',
        'Desc.Art.Ripley': 'Descripción',
        'Precio Unitario': 'Precio Normal',
        'Cod.Art. Venta': 'Código Barra',
        'Cod. Departamento': 'Cod Departamento',
        'Cod.Art. Ripley': 'Cod Producto Cliente',
        'Fecha Generacion': 'Fecha Emisión',
        'Cod. Linea': 'Temporada',
    }

    COLUMNS_WHIT_DATE = {
        'Fecha Vto': '%Y-%m-%d 00:00:00',
        'Fecha Emisión': '%Y-%m-%d 00:00:00',
    }

    @classmethod
    def settings(cls):
        username = os.environ.get(cls.string_variables['username'], '')
        password = os.environ.get(cls.string_variables['password'], '')
        empresa = os.environ.get(cls.string_variables['empresa'], '')
        storage_path = os.environ['STORAGE_PATH']
        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': empresa,
        }


class RipleyB2BOCTest(
        B2BWebOC,
        RipleyB2BOCTestWebSource,
        SimpleDailyCsvStorage,
        DailyFetcher):

    name = 'b2b-ripley-oc-test'

    string_variables = {
        'username': 'B2B_USERNAME_RIPLEY',
        'password': 'B2B_PASSWORD_RIPLEY',
        'empresa': 'B2B_EMPRESA_RIPLEY',
    }

    PORTAL = 'Ripley'

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
        'Número de OC': 'Número de OC',
        'Cód. Art. Prov.   (Case Pack)': 'SKU',
        'Cod. Sucursal de Destino': 'Cod Sucursal de Destino',
        'Solicitado (u)': 'Solicitado',
        'Costo Neto Unitario': 'Costo Neto Unitario',
        'Fecha Cancelacion': 'Fecha Vto',
        'Desc.Art.Ripley': 'Descripción',
        'Precio Unitario': 'Precio Normal',
        'Cod.Art. Venta': 'Código Barra',
        'Cod. Departamento': 'Cod Departamento',
        'Cod.Art. Ripley': 'Cod Producto Cliente',
        'Fecha Generacion': 'Fecha Emisión',
        'Cod. Linea': 'Temporada',
    }

    COLUMNS_WHIT_DATE = {
        'Fecha Vto': '%Y-%m-%d 00:00:00',
        'Fecha Emisión': '%Y-%m-%d 00:00:00',
    }

    @classmethod
    def settings(cls):
        username = os.environ.get(cls.string_variables['username'], '')
        password = os.environ.get(cls.string_variables['password'], '')
        empresa = os.environ.get(cls.string_variables['empresa'], '')
        storage_path = os.environ['STORAGE_PATH']
        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': empresa,
        }


class RipleyB2BCustomSales(
        B2BWebOC,
        RipleyB2BFileSource,
        SimpleDailyCsvStorage,
        DailyFetcher):

    name = 'b2b-ripley-custom-sales'

    DATA_NAME = 'sales'

    string_variables = {
        'username': 'B2B_USERNAME_RIPLEY',
        'password': 'B2B_PASSWORD_RIPLEY',
        'empresa': 'B2B_EMPRESA_RIPLEY',
        'repository_path': 'SOURCE_INT_PATH',
    }

    PORTAL = 'Ripley'

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
        'Cod. Sucursal': 'Cod Tienda',
        'Sucursal': 'Tienda',
        'Cód.Art. Ripley': 'Cod Producto',
        'Desc.Art.Ripley': 'Producto',
        'Departamento': 'Categoria',
        'Marca': 'Marca',
        'Cód. Art. Prov. (Case Pack)': 'Cod Modelo',
        'Venta Valorizada($)': 'Venta',
        'Unidades Vendidas': 'Unidades Vendidas',
        'Stock on Hand ($)': 'Stock Valorizado',
        'Stock on Hand (u)': 'Unidades de Stock'
    }

    COLUMNS_WHIT_DATE = {}

    @classmethod
    def settings(cls):
        username = os.environ.get(cls.string_variables['username'], '')
        password = os.environ.get(cls.string_variables['password'], '')
        empresa = os.environ.get(cls.string_variables['empresa'], '')
        storage_path = os.environ['STORAGE_PATH']
        repository_path = os.environ.get(cls.string_variables['repository_path'])
        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': empresa,
            'repository_path': repository_path,
        }


class RipleyB2BSales(B2BFile, RipleyB2BFileSource, OnlyCsvStorage, DailyFetcher):

    name = 'b2b-ripley-files'

    string_variables = {
        'username': 'B2B_USERNAME_RIPLEY',
        'empresa': 'B2B_EMPRESA_RIPLEY',
        'repository_path': 'SOURCE_INT_PATH',
    }

    PRODUCT_SUFFIX = ''
    STORE_NAME_SUFFIX = ''
    STORE_NAME_PREFIX = ''
    IVA = 1.19

    PORTAL = 'Ripley'

    date_format = '%Y-%m-%d %H:%M:%S 00:00'

    @classmethod
    def settings(cls):
        variables = super().settings()
        empresa = os.environ.get(cls.string_variables['empresa'], '')
        repository_path = os.environ.get(cls.string_variables['repository_path'], '')
        variables['repository_path'] = repository_path
        variables['b2b_empresa'] = empresa

        return variables

    def base_data(self, row, date_time):

        b2bproduct_id = row['Cód.Art. Ripley']
        product_name = row['Desc.Art.Ripley']
        self.chain_id = 'ripley'
        chain_name = 'Ripley'
        cod_sucursal = row['Cod. Sucursal']
        store_id = self.chain_id + cod_sucursal
        store_name_prefix = '{} '.format(
            self.STORE_NAME_PREFIX) if self.STORE_NAME_PREFIX else ''
        store_name = store_name_prefix + \
            row['Sucursal'] + ' {}'.format(self.STORE_NAME_SUFFIX)
        brand_id = self.chain_id + create_id(row['Marca'])
        brand_name = row['Marca']
        category_id = row['Cod. Departamento']
        category_name = row['Departamento']
        codmodel = row['Cód. Art. Prov. (Case Pack)']

        base_data = {
            self.mapping_column_name('brand_id'): brand_id,
            self.mapping_column_name('brand_name'): brand_name,
            self.mapping_column_name('source_id'): self.chain_id,
            self.mapping_column_name('source_name'): chain_name,
            self.mapping_column_name('store_id'): store_id,
            self.mapping_column_name('store_name'): store_name,
            self.mapping_column_name('product_id'): self.chain_id + self.PRODUCT_SUFFIX + b2bproduct_id,
            self.mapping_column_name('product_name'): product_name,
            self.mapping_column_name('category_id'): self.chain_id + category_id,
            self.mapping_column_name('category_name'): category_name,
            self.mapping_column_name('codstore_id'): store_id,
            self.mapping_column_name('codstore_name'): cod_sucursal,
            self.mapping_column_name('codproduct_id'): self.chain_id + b2bproduct_id,
            self.mapping_column_name('codproduct_name'): b2bproduct_id,
            self.mapping_column_name('codmodel_id'): self.chain_id + codmodel,
            self.mapping_column_name('codmodel_name'): codmodel,
            'datetime': datetime_to_wivo_format(date_time)}

        return base_data

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):

        net_values = kwargs['net_values']
        allow_sales_zero = bool(kwargs.get('allow_sales_zero', True))

        saleunits_value = {
            'value': row['Unidades Vendidas']
        }

        if net_values:
            sales = row['Venta Valorizada($)']
        else:
            sales = float(row['Venta Valorizada($)']) * self.IVA

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


class RipleyB2BStock(RipleyB2BStockSource, RipleyB2BSales):

    name = 'b2b-ripley-stock-files'

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):

        net_values = kwargs['net_values']

        stockunits_value = {
            'value': row['Stock on Hand (u)']
        }

        if net_values:
            stock = row['Stock on Hand ($)']
        else:
            stock = float(row['Stock on Hand ($)']) * self.IVA

        stocks_value = {
            'value': stock
        }
        self.append(metrics['stockunit'], date_time, {**base_data, **stockunits_value})
        self.append(metrics['stock'], date_time, {**base_data, **stocks_value})


class RipleyB2BPortal(
        B2BPortalBase,
        RipleyB2BPortalSource,
        B2BWebFetcher):

    name = 'b2b-ripley-portal'

    string_variables = {
        'username': 'B2B_USERNAME_RIPLEY',
        'password': 'B2B_PASSWORD_RIPLEY',
        'empresa': 'B2B_EMPRESA_RIPLEY',
    }

    related_fetchers = [RipleyB2BSales]

    @classmethod
    def settings(cls):
        args = super().settings()
        empresa = os.environ.get(cls.string_variables['empresa'], '')
        args['b2b_empresa'] = empresa

        return args


class RipleyB2BPeruSales(RipleyB2BSales):

    name = 'b2b-ripley-peru-files'

    IVA = 1.18


class Ripleyb2BPeruStock(RipleyB2BStock):

    name = 'b2b-ripley-peru-stock-files'

    IVA = 1.18


class RipleyB2BPeru(RipleyB2BPeruPortalSource, RipleyB2BPortal):

    name = 'b2b-ripley-peru-portal'

    related_fetchers = [RipleyB2BPeruSales]
