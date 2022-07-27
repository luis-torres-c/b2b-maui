import os
import datetime

from core.sources.b2b.b2bfalabella import FalabellaB2BPortalSource, FalabellaB2BFileSource, FalabellaB2BStockSource, FalabellaB2BPeruPortalSource, FalabellaB2BPeruFileSource, FalabellaB2BOCWebSource
from core.storages.onlycsv import OnlyCsvStorage
from core.fetcher.b2b.base import B2BPortalBase, B2BFile
from core.fetcher.base import B2BWebFetcher, DailyFetcher
from core.fetcher.b2b.base import B2BWebOC
from core.utils import datetime_to_wivo_format, create_id
from core.storages.simple import SimpleDailyCsvStorage


class FalabellaB2BOC(B2BWebOC, FalabellaB2BOCWebSource, SimpleDailyCsvStorage, DailyFetcher):

    name = 'b2b-falabella-oc'

    string_variables = {
        'username': 'B2B_USERNAME_FALABELLA',
        'password': 'B2B_PASSWORD_FALABELLA',
        'empresa': 'B2B_EMPRESA_FALABELLA',
    }

    PORTAL = 'Falabella'

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
        'NRO_OC': 'Número de OC',
        'SKU': 'Cod Producto Cliente',
        'NRO_LOCAL': 'Cod Sucursal de Destino',
        'UNIDADES': 'Solicitado',
        'COSTO_UNI': 'Costo Neto Unitario',
        'FECHA_HASTA': 'Fecha Vto',
        'DESCRIPCION_LARGA': 'Descripción',
        'PRECIO_UNI': 'Precio Normal',
        'UPC': 'Código Barra',
        'FECHA_EMISION': 'Fecha Emisión',
        'MODELO': 'SKU',
        'SUBLINEA': 'Cod Departamento',
    }

    COLUMNS_WHIT_DATE = {
        'Fecha Vto': '%d/%m/%Y',
        'Fecha Emisión': '%d/%m/%Y',
    }

    @classmethod
    def settings(cls):
        variables = super().settings()
        empresa = os.environ.get(cls.string_variables['empresa'], '')
        variables['b2b_empresa'] = empresa

        return variables


class FalabellaB2BCustomSales(
        B2BWebOC,
        FalabellaB2BFileSource,
        SimpleDailyCsvStorage,
        DailyFetcher):

    name = 'b2b-falabella-custom-sales'

    DATA_NAME = 'sales'

    string_variables = {
        'username': 'B2B_USERNAME_FALABELLA',
        'password': 'B2B_PASSWORD_FALABELLA',
        'empresa': 'B2B_EMPRESA_FALABELLA',
        'by_week': 'BY_WEEK_FALABELLA',
        'repository_path': 'SOURCE_INT_PATH',
    }

    PORTAL = 'Falabella'

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
        'NRO_LOCAL': 'Cod Tienda',
        'LOCAL': 'Tienda',
        'SKU': 'Cod Producto',
        'DESCRIPCION_LARGA': 'Producto',
        'DESC_SUBCLASE': 'Categoria',
        'MARCA': 'Marca',
        'MODELO': 'Cod Modelo',
        'MONTO_VENTA': 'Venta',
        'VENTA_UNIDAD': 'Unidades Vendidas',
        'STOCK_VALOR': 'Stock Valorizado',
        'STOCK': 'Unidades de Stock'
    }

    COLUMNS_WHIT_DATE = {}

    @classmethod
    def settings(cls):
        variables = super().settings()
        empresa = os.environ.get(cls.string_variables['empresa'], '')
        by_week = os.environ.get(cls.string_variables['by_week'], 1)
        variables['by_week'] = bool(int(by_week))
        variables['b2b_empresa'] = empresa
        variables['repository_path'] = os.environ.get(cls.string_variables['repository_path'])

        return variables

    def parse_data(self, connector, **kwargs):

        data = connector.detalle_venta()
        data_result = list()

        by_week = kwargs['by_week']

        for row in data:
            new_row = dict()
            for key in self.BASE_COLUMN_NAMES:
                new_row[key] = ''
            new_row['Cliente'] = self.PORTAL
            for key in self.PARSE_COLUMN_NAMES.keys():
                if isinstance(self.PARSE_COLUMN_NAMES[key], list):
                    for k in self.PARSE_COLUMN_NAMES[key]:
                        new_row[k] = row[key] if key in row else ''
                else:
                    new_row[self.PARSE_COLUMN_NAMES[key]] = row[key] if key in row else ''
            for key in new_row.keys():
                if key in self.COLUMNS_WHIT_DATE.keys():
                    new_row[key] = datetime.datetime.strptime(new_row[key], self.COLUMNS_WHIT_DATE[key]).strftime(self.date_format) if new_row[key] != '' else new_row[key]
            if by_week:
                for day in row['STRING_DAYS']:
                    diferent_day = {
                        'Venta': row['MONTO_VENTA_' + day],
                        'Unidades Vendidas': row['VENTA_UNIDAD_' + day],
                        'Fecha': row['Datetime_' + day]
                    }
                    data_result.append({**new_row, **diferent_day})
            else:
                data_result.append(new_row)

        if by_week:
            all_results = dict()
            for row in data_result:
                if row['Fecha'] in all_results.keys():
                    all_results[row['Fecha']].append(row)
                else:
                    all_results[row['Fecha']] = [row, ]
            for key in all_results.keys():
                for row in all_results[key]:
                    row['Fecha'] = str(row['Fecha'])
            result = list()
            for key in all_results.keys():
                if key.date() == self._actual_date:
                    for r in all_results[key]:
                        del(r['Fecha'])
                    result.append({
                        'type': 'simple-data',
                        'data_name': self.DATA_NAME,
                        'records': all_results[key],
                        'representative_date': key.date(),
                    })
            return result

        else:
            return [{
                'type': 'simple-data',
                'data_name': self.DATA_NAME,
                'records': data_result,
            }, ]


class FalabellaB2BPeruPortal(
        B2BPortalBase,
        FalabellaB2BPeruPortalSource,
        B2BWebFetcher):

    name = 'b2b-falabella-peru-portal'


class FalabellaB2BPeruSales(B2BFile, FalabellaB2BPeruFileSource, OnlyCsvStorage, DailyFetcher):

    name = 'b2b-falabella-peru-files'

    string_variables = {
        'username': 'B2B_USERNAME_FALABELLA',
        'empresa': 'B2B_EMPRESA_FALABELLA',
        'repository_path': 'SOURCE_INT_PATH',
    }

    PRODUCT_SUFFIX = ''
    STORE_NAME_SUFFIX = ''
    STORE_NAME_PREFIX = ''
    IVA = 1.18

    PORTAL = 'Falabella'

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
        b2bproduct_id = row['SKU'].strip()
        product_name = row['DESCRIPCION_LARGA'].strip()
        store_id = row['NRO_LOCAL'].strip()
        store_name_prefix = '{} '.format(self.STORE_NAME_PREFIX) if self.STORE_NAME_PREFIX else ''
        store_name = store_name_prefix + row['LOCAL'].strip() + ' {}'.format(self.STORE_NAME_SUFFIX)
        self.chain_id = 'falabella'
        chain_name = 'Falabella'
        brand_id = self.chain_id + create_id(row['MARCA'])
        brand_name = row['MARCA'].strip()
        codmodel = row['MODELO'].strip()
        category_id = row['SUBCLASE'].strip()
        category_name = row['DESC_SUBCLASE'].strip()

        base_data = {
            self.mapping_column_name('brand_id'): brand_id,
            self.mapping_column_name('brand_name'): brand_name,
            self.mapping_column_name('source_id'): self.chain_id,
            self.mapping_column_name('source_name'): chain_name,
            self.mapping_column_name('store_id'): self.chain_id + store_id,
            self.mapping_column_name('store_name'): store_name,
            self.mapping_column_name('product_id'): self.chain_id + self.PRODUCT_SUFFIX + b2bproduct_id,
            self.mapping_column_name('product_name'): product_name,
            self.mapping_column_name('category_id'): self.chain_id + category_id,
            self.mapping_column_name('category_name'): category_name,
            self.mapping_column_name('codstore_id'): self.chain_id + store_id,
            self.mapping_column_name('codstore_name'): store_id,
            self.mapping_column_name('codproduct_id'): self.chain_id + b2bproduct_id,
            self.mapping_column_name('codproduct_name'): b2bproduct_id,
            self.mapping_column_name('codmodel_id'): self.chain_id + codmodel,
            self.mapping_column_name('codmodel_name'): codmodel,
            'datetime': datetime_to_wivo_format(date_time),
        }

        return base_data

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):

        net_values = kwargs['net_values']
        by_week = not('VENTA_UNIDAD_DIA' in row)

        if by_week:
            days = row['STRING_DAYS']
            for day in days:
                dt = row['Datetime_' + day]
                if net_values:
                    sale = format(row['MONTO_VENTA_' + day] / self.IVA,
                                  '.2f') if row['MONTO_VENTA_' + day] != 0 else 0
                else:
                    sale = row['MONTO_VENTA_' + day]

                saleunit = row['VENTA_UNIDAD_' + day]

                sales_value = {
                    'value': sale
                }
                if sale == 0:
                    saleunit = 0

                saleunits_value = {
                    'value': saleunit
                }

                base_data['datetime'] = datetime_to_wivo_format(dt)
                if dt == date_time:
                    if sales_value['value'] != 0:
                        self.append(metrics['sale'], dt, {**base_data, **sales_value})
                    if saleunits_value['value'] != 0:
                        self.append(metrics['salesunit'], dt, {**base_data, **saleunits_value})

        else:
            if net_values:
                sale = format(
                    row['MONTO_VENTA'] / self.IVA,
                    '.2f') if row['MONTO_VENTA'] != 0 else 0
            else:
                sale = row['MONTO_VENTA']

            sales_value = {
                'value': sale
            }
            saleunits_value = {
                'value': row['VENTA_UNIDAD_DIA']
            }

            if sales_value['value'] != 0:
                self.append(metrics['sale'], date_time, {**base_data, **sales_value})
            if saleunits_value['value'] != 0:
                self.append(metrics['salesunit'], date_time, {**base_data, **saleunits_value})


class FalabellaB2BSales(B2BFile, FalabellaB2BFileSource, OnlyCsvStorage, DailyFetcher):

    name = 'b2b-falabella-files'

    string_variables = {
        'username': 'B2B_USERNAME_FALABELLA',
        'empresa': 'B2B_EMPRESA_FALABELLA',
        'repository_path': 'SOURCE_INT_PATH',
    }

    PRODUCT_SUFFIX = ''
    STORE_NAME_SUFFIX = ''
    STORE_NAME_PREFIX = ''
    IVA = 1.19

    PORTAL = 'Falabella'

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
        b2bproduct_id = row['SKU'].strip()
        product_name = row['DESCRIPCION_LARGA'].strip()
        store_id = row['NRO_LOCAL'].strip()
        store_name_prefix = '{} '.format(self.STORE_NAME_PREFIX) if self.STORE_NAME_PREFIX else ''
        store_name = store_name_prefix + row['LOCAL'].strip() + ' {}'.format(self.STORE_NAME_SUFFIX)
        self.chain_id = self.PORTAL.lower()
        chain_name = self.PORTAL
        brand_id = self.chain_id + create_id(row['MARCA'])
        brand_name = row['MARCA'].strip()
        codmodel = row['MODELO'].strip()
        category_id = row['SUBCLASE'].strip()
        category_name = row['DESC_SUBCLASE'].strip()

        base_data = {
            self.mapping_column_name('brand_id'): brand_id,
            self.mapping_column_name('brand_name'): brand_name,
            self.mapping_column_name('source_id'): self.chain_id,
            self.mapping_column_name('source_name'): chain_name,
            self.mapping_column_name('store_id'): self.chain_id + store_id,
            self.mapping_column_name('store_name'): store_name,
            self.mapping_column_name('product_id'): self.chain_id + self.PRODUCT_SUFFIX + b2bproduct_id,
            self.mapping_column_name('product_name'): product_name,
            self.mapping_column_name('category_id'): self.chain_id + category_id,
            self.mapping_column_name('category_name'): category_name,
            self.mapping_column_name('codstore_id'): self.chain_id + store_id,
            self.mapping_column_name('codstore_name'): store_id,
            self.mapping_column_name('codproduct_id'): self.chain_id + b2bproduct_id,
            self.mapping_column_name('codproduct_name'): b2bproduct_id,
            self.mapping_column_name('codmodel_id'): self.chain_id + codmodel,
            self.mapping_column_name('codmodel_name'): codmodel,
            'datetime': datetime_to_wivo_format(date_time),
        }

        return base_data

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):

        net_values = kwargs['net_values']
        allow_sales_zero = bool(kwargs.get('allow_sales_zero', True))
        by_week = not('VENTA_UNIDAD_DIA' in row)

        if by_week:
            days = row['STRING_DAYS']
            for day in days:
                dt = row['Datetime_' + day]
                if net_values:
                    sale = row['MONTO_VENTA_' + day] / self.IVA
                else:
                    sale = row['MONTO_VENTA_' + day]

                saleunit = row['VENTA_UNIDAD_' + day]

                sales_value = {
                    'value': sale
                }
                if sale == 0:
                    saleunit = 0

                saleunits_value = {
                    'value': saleunit
                }

                base_data['datetime'] = datetime_to_wivo_format(dt)

                if dt == date_time:
                    if allow_sales_zero:
                        self.append(metrics['sale'], dt, {**base_data, **sales_value})
                        self.append(metrics['salesunit'], dt, {**base_data, **saleunits_value})
                    else:
                        if sales_value['value'] != 0:
                            self.append(metrics['sale'], dt, {**base_data, **sales_value})
                        if saleunits_value['value'] != 0:
                            self.append(metrics['salesunit'], dt, {**base_data, **saleunits_value})

        else:
            sale = row['MONTO_VENTA']
            if net_values:
                sale = row['MONTO_VENTA'] / self.IVA

            sales_value = {
                'value': sale
            }
            saleunits_value = {
                'value': row['VENTA_UNIDAD_DIA']
            }

            if allow_sales_zero:
                self.append(metrics['sale'], date_time, {**base_data, **sales_value})
                self.append(metrics['salesunit'], date_time, {**base_data, **saleunits_value})
            else:
                if sales_value['value'] != 0:
                    self.append(metrics['sale'], date_time, {**base_data, **sales_value})
                if saleunits_value['value'] != 0:
                    self.append(metrics['salesunit'], date_time, {**base_data, **saleunits_value})


class FalabellaB2BStockFile(FalabellaB2BStockSource, FalabellaB2BSales):

    name = 'b2b-falabella-stock-files'

    string_variables = {
        'username': 'B2B_USERNAME_FALABELLA',
        'empresa': 'B2B_EMPRESA_FALABELLA',
        'repository_path': 'SOURCE_INT_PATH',
        'stock_iva': 'STOCK_IVA_FALABELLA',
    }

    @classmethod
    def settings(cls):
        variables = super().settings()
        stock_iva = os.environ.get(cls.string_variables['stock_iva'], 0)
        variables['stock_iva'] = bool(int(stock_iva))

        return variables

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):

        stock_iva = kwargs['stock_iva']

        stockunits_value = {
            'value': row['STOCK']
        }

        if stock_iva:
            stock = row['STOCK_VALOR']
        else:
            stock = format(row['STOCK_VALOR'] / self.IVA, '.2f') if row['STOCK_VALOR'] != 0 else 0

        stocks_value = {
            'value': stock
        }
        self.append(metrics['stockunit'], date_time, {**base_data, **stockunits_value})
        self.append(metrics['stock'], date_time, {**base_data, **stocks_value})


class FalabellaB2BPeruStockFile(FalabellaB2BStockFile):

    IVA = 1.18

    name = 'b2b-falabella-peru-stock-files'


class FalabellaB2BPortal(
        B2BPortalBase,
        FalabellaB2BPortalSource,
        B2BWebFetcher):

    name = 'b2b-falabella-portal'
    related_fetchers = [FalabellaB2BSales]

    string_variables = {
        'username': 'B2B_USERNAME_FALABELLA',
        'repository_path': 'SOURCE_INT_PATH',
        'password': 'B2B_PASSWORD_FALABELLA',
        'empresa': 'B2B_EMPRESA_FALABELLA',
    }

    @classmethod
    def settings(cls):
        args = super().settings()
        args['b2b_empresa'] = os.environ.get(cls.string_variables['empresa'], '')
        return args
