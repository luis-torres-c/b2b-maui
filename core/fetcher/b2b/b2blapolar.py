import os

from core.fetcher import DailyFetcher
from core.fetcher.b2b.base import B2BFile
from core.fetcher.b2b.base import B2BPortalBase
from core.fetcher.b2b.base import B2BWebOC
from core.fetcher.base import B2BWebFetcher
from core.sources.b2b.b2blapolar import LaPolarB2BOCWebSource
from core.sources.b2b.b2blapolar import LaPolarB2BFileSource
from core.sources.b2b.b2blapolar import LaPolarB2BPortalSource
from core.sources.b2b.b2blapolar import LaPolarB2BStockSource
from core.storages.onlycsv import OnlyCsvStorage
from core.storages.simple import SimpleDailyCsvStorage
from core.utils import datetime_to_wivo_format


class LaPolarB2BOC(
        B2BWebOC,
        LaPolarB2BOCWebSource,
        SimpleDailyCsvStorage,
        DailyFetcher):

    name = 'b2b-lapolar-oc'

    string_variables = {
        'username': 'B2B_USERNAME_LAPOLAR',
        'password': 'B2B_PASSWORD_LAPOLAR',
        'empresa': 'B2B_EMPRESA_LAPOLAR',
    }

    PORTAL = 'La Polar'

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
        'Fecha Emisión']
    PARSE_COLUMN_NAMES = {
        'No OC': 'Número de OC',
        'Cód. Prov.': 'SKU',
        'Cód. Local Destino': 'Cod Sucursal de Destino',
        'Unidades Solicitadas': 'Solicitado',
        'Costo Unitario': 'Costo Neto Unitario',
        'Fecha Vto.': 'Fecha Vto',
        'Cód. Local Entrega': 'Cod Departamento',
        'Desc. Producto': 'Descripción',
        'Precio Venta': 'Precio Normal',
        'Cód. Barra': 'Código Barra',
        'Articulo': 'Cod Producto Cliente',
        'Fecha Emisión': 'Fecha Emisión',
        'PLU': 'Temporada',
    }

    @classmethod
    def settings(cls):
        args = super().settings()
        empresa = os.environ.get(cls.string_variables['empresa'], '')
        args['b2b_empresa'] = empresa
        return args


class LaPolarB2BCustomSales(
        B2BWebOC,
        LaPolarB2BFileSource,
        SimpleDailyCsvStorage,
        DailyFetcher):

    name = 'b2b-lapolar-custom-sales'

    DATA_NAME = 'sales'

    string_variables = {
        'username': 'B2B_USERNAME_LAPOLAR',
        'password': 'B2B_PASSWORD_LAPOLAR',
        'empresa': 'B2B_EMPRESA_LAPOLAR',
        'repository_path': 'SOURCE_INT_PATH',
    }

    PORTAL = 'La Polar'

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
        'PLU': 'Cod Producto',
        'DESCRIPCION': 'Producto',
        'CATPRODN3': 'Categoria',
        'CATPRODN5': 'Marca',
        'COD_PROVEEDOR': 'Cod Modelo',
        'VENTA_PERIODO(MC)': 'Venta',
        'VENTA_PERIODO(U)': 'Unidades Vendidas',
        'INV_DISPONIBLE($)': 'Stock Valorizado',
        'INV_DISP(U)': 'Unidades de Stock'
    }

    COLUMNS_WHIT_DATE = {}

    @classmethod
    def settings(cls):
        args = super().settings()
        empresa = os.environ.get(cls.string_variables['empresa'], '')
        repository_path = os.environ.get(cls.string_variables['repository_path'], '')
        args['repository_path'] = repository_path
        args['b2b_empresa'] = empresa
        return args


class LaPolarB2BSales(B2BFile, LaPolarB2BFileSource, OnlyCsvStorage, DailyFetcher):

    name = 'b2b-lapolar-files'

    date_format = '%Y-%m-%d 00:00:00 00:00'
    STORE_NAME_PREFIX = ''
    PRODUCT_SUFFIX = ''
    IVA = 1.19
    PORTAL = 'La Polar'

    string_variables = {
        'username': 'B2B_USERNAME_LAPOLAR',
        'empresa': 'B2B_EMPRESA_LAPOLAR',
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
        b2bproduct_id = row['PLU']
        product_name = row['DESCRIPCION']
        store_id = row['COD_LOCAL']
        store_name_prefix = '{} '.format(
            self.STORE_NAME_PREFIX) if self.STORE_NAME_PREFIX else ''
        store_name = store_name_prefix + row['DESCRIPCION_LOCAL']
        self.chain_id = 'lapolar'
        chain_name = 'La Polar'
        codmodel = row['COD_PROVEEDOR']
        category_name = row['CATPRODN3']
        # TODO: check if this value is valid (.split('  ', 2)[2])
        brand_name = row['Marca']

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

        allow_sales_zero = bool(kwargs.get('allow_sales_zero', True))

        saleunits_value = {
            'value': row['VENTA_PERIODO(U)']
        }

        sales = row['VENTA_PERIODO(MV)']
        if not kwargs['net_values']:
            sales = row['VENTA_PERIODO(MV)'] * self.IVA

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


class LaPolarB2BStock(LaPolarB2BStockSource, LaPolarB2BSales):

    name = 'b2b-lapolar-stock-files'

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):

        if 'INV_DISP(U)' in row:
            stockunits_value = {
                'value': row['INV_DISP(U)']
            }

            stock = row['INV_DISPONIBLE($)']
            if not kwargs['net_values']:
                stock = row['INV_DISPONIBLE($)'] * self.IVA

            stocks_value = {
                'value': stock
            }
            self.append(metrics['stockunit'], date_time, {**base_data, **stockunits_value})
            self.append(metrics['stock'], date_time, {**base_data, **stocks_value})


class LaPolarB2B(
        B2BPortalBase,
        LaPolarB2BPortalSource,
        B2BWebFetcher):

    name = 'b2b-lapolar-portal'

    related_fetchers = [LaPolarB2BSales]

    string_variables = {
        'username': 'B2B_USERNAME_LAPOLAR',
        'password': 'B2B_PASSWORD_LAPOLAR',
        'empresa': 'B2B_EMPRESA_LAPOLAR'
    }

    @classmethod
    def settings(cls):
        args = super().settings()
        empresa = os.environ.get(cls.string_variables['empresa'], '')
        args['b2b_empresa'] = empresa
        return args
