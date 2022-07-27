import os

from core.fetcher import DailyFetcher
from core.fetcher.b2b.base import B2BFile
from core.fetcher.b2b.base import B2BPortalBase
from core.fetcher.b2b.base import B2BWebOC
from core.fetcher.base import B2BWebFetcher
from core.sources.b2b.b2bparis import ParisB2BFileSource
from core.sources.b2b.b2bparis import ParisB2BOCWebSource
from core.sources.b2b.b2bparis import ParisB2BPortalSource
from core.sources.b2b.b2bparis import ParisB2BStockSource
from core.storages.onlycsv import OnlyCsvStorage
from core.storages.simple import SimpleDailyCsvStorage
from core.utils import create_id
from core.utils import datetime_to_wivo_format


class ParisB2BOC(B2BWebOC, ParisB2BOCWebSource, SimpleDailyCsvStorage, DailyFetcher):

    name = 'b2b-paris-oc'

    string_variables = {
        'username': 'B2B_USERNAME_PARIS',
        'password': 'B2B_PASSWORD_PARIS',
        'empresa': 'B2B_EMPRESA_PARIS',
    }

    PORTAL = 'Paris'

    BASE_COLUMN_NAMES = ['Cliente', 'Número de OC', 'SKU', 'Cod Sucursal de Destino', 'EMPRESA', 'Solicitado', 'Costo Neto Unitario', 'Fecha Vto', 'Descripción', 'Precio Normal', 'Código Barra', 'Cod Departamento', 'Departamento', 'DV', 'Talla', 'Cod Talla', 'Estilo', 'Desc Color', 'Temporada', 'Color', 'Cod Producto Cliente', 'Fecha Emisión']
    PARSE_COLUMN_NAMES = {
        'N° Orden': 'Número de OC',
        'Cód. Prod. Prov.': 'SKU',
        'Cód. Local Destino': 'Cod Sucursal de Destino',
        'Solicitado': 'Solicitado',
        'Costo Neto': 'Costo Neto Unitario',
        'Fecha Vto.': 'Fecha Vto',
        'Descripción': 'Descripción',
        'Precio Normal': 'Precio Normal',
        'Cód. Barra Paris': 'Código Barra',
        'Cód. Departamento': 'Cod Departamento',
        'Departamento': 'Departamento',
        'SKU Paris': 'Cod Producto Cliente',
        'Emitida': 'Fecha Emisión'
    }

    COLUMNS_WHIT_DATE = {
        'Fecha Vto': '%Y-%m-%d',
        'Fecha Emisión': '%Y-%m-%d',
    }

    @classmethod
    def settings(cls):
        variables = super().settings()
        empresa = os.environ.get(cls.string_variables['empresa'], '')
        variables['b2b_empresa'] = empresa

        return variables


class ParisB2BCustomSales(
        B2BWebOC,
        ParisB2BFileSource,
        SimpleDailyCsvStorage,
        DailyFetcher):

    name = 'b2b-paris-custom-sales'

    DATA_NAME = 'sales'

    string_variables = {
        'username': 'B2B_USERNAME_PARIS',
        'password': 'B2B_PASSWORD_PARIS',
        'empresa': 'B2B_EMPRESA_PARIS',
        'repository_path': 'SOURCE_INT_PATH',
    }

    PORTAL = 'Paris'

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
        'COD_CENCOSUD': 'Cod Producto',
        'DESCRIPCION_PRODUCTO': 'Producto',
        'SUBDEPARTAMENTO': 'Categoria',
        'MARCA': 'Marca',
        'COD_PROVEEDOR': 'Cod Modelo',
        'VTA_PERIODO_PUBLICO($)': 'Venta',
        'VTA_PERIODO(u)': 'Unidades Vendidas',
        'INVENTARIO($)': 'Stock Valorizado',
        'INVENTARIO(u)': 'Unidades de Stock'
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


class ParisB2BSales(B2BFile, ParisB2BFileSource, OnlyCsvStorage, DailyFetcher):

    name = 'b2b-paris-files'

    date_format = '%Y-%m-%d 00:00:00 00:00'
    PRODUCT_SUFFIX = ''
    STORE_NAME_PREFIX = ''
    ALLOWED_CHAIN_IDS = [
        'johnson',
        'paris',
    ]

    DISABLE_BRAND = []

    string_variables = {
        'username': 'B2B_USERNAME_PARIS',
        'empresa': 'B2B_EMPRESA_PARIS',
        'repository_path': 'SOURCE_INT_PATH',
    }

    IVA = 1.19

    PORTAL = 'Paris'

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
        dif = store_id[0:3]
        store_name_prefix = '{} '.format(self.STORE_NAME_PREFIX) if self.STORE_NAME_PREFIX else ''
        store_name = store_name_prefix + store_name
        self.chain_id = 'johnson' if dif == '004' else 'paris'
        chain_name = 'Johnson' if dif == '004' else 'Paris'
        brand_id = self.chain_id + create_id(row['MARCA'])
        brand_name = row['MARCA']
        codmodel = row['COD_PROVEEDOR']
        category_name = row['SUBDEPARTAMENTO'] if 'SUBDEPARTAMENTO' in row else 'SIN CATEGORIA'

        # Mechanism to filter chains in others cases.
        if self.chain_id not in self.ALLOWED_CHAIN_IDS:
            return {}

        # brand filter
        if brand_name in self.DISABLE_BRAND:
            return {}

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
            'datetime': datetime_to_wivo_format(date_time)
        }

        return base_data

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):

        net_values = kwargs['net_values']
        allow_sales_zero = bool(kwargs.get('allow_sales_zero', True))

        sale_value = row['VTA_PERIODO_PUBLICO($)']
        if net_values:
            sale_value = row['VTA_PERIODO_PUBLICO($)'] / self.IVA

        saleunit_value = row['VTA_PERIODO(u)']

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


class ParisB2BStock(ParisB2BStockSource, ParisB2BSales):

    name = 'b2b-paris-stock-files'

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):

        net_values = kwargs['net_values']

        if 'INVENTARIO(u)' in row:
            stockunits_value = {
                'value': row['INVENTARIO(u)']
            }
            stock = row['INVENTARIO($)']
            if not net_values:
                stock = row['INVENTARIO($)'] * self.IVA

            stocks_value = {
                'value': stock
            }
            self.append(metrics['stockunit'], date_time, {**base_data, **stockunits_value})
            self.append(metrics['stock'], date_time, {**base_data, **stocks_value})


class ParisB2B(
        B2BPortalBase,
        ParisB2BPortalSource,
        B2BWebFetcher):

    name = 'b2b-paris-portal'

    string_variables = {
        'username': 'B2B_USERNAME_PARIS',
        'password': 'B2B_PASSWORD_PARIS',
        'empresa': 'B2B_EMPRESA_PARIS',
    }

    PORTAL = 'Paris'

    related_fetchers = [ParisB2BSales]

    @classmethod
    def settings(cls):
        variables = super().settings()
        empresa = os.environ.get(cls.string_variables['empresa'], '')
        variables['b2b_empresa'] = empresa
        return variables
