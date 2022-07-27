import os

from core.sources.b2b.b2bsodimac import SodimacB2BPortalSource
from core.sources.b2b.b2bsodimac import SodimacB2BFileSource
from core.sources.b2b.b2bsodimac import SodimacB2BStockSource
from core.storages.onlycsv import OnlyCsvStorage
from core.fetcher.b2b.base import B2BPortalBase, B2BFile
from core.fetcher.base import B2BWebFetcher, DailyFetcher
from core.utils import datetime_to_wivo_format, create_id


class SodimacB2BSales(B2BFile, SodimacB2BFileSource, OnlyCsvStorage, DailyFetcher):

    name = 'b2b-sodimac-files'

    string_variables = {
        'username': 'B2B_USERNAME_SODIMAC',
        'empresa': 'B2B_EMPRESA_SODIMAC',
        'repository_path': 'SOURCE_INT_PATH',
    }

    PRODUCT_SUFFIX = ''
    STORE_NAME_SUFFIX = ''
    STORE_NAME_PREFIX = ''
    IVA = 1.19

    PORTAL = 'Sodimac'

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


class SodimacB2BStockFile(SodimacB2BStockSource, SodimacB2BSales):

    name = 'b2b-sodimac-stock-files'

    string_variables = {
        'username': 'B2B_USERNAME_SODIMAC',
        'empresa': 'B2B_EMPRESA_SODIMAC',
        'repository_path': 'SOURCE_INT_PATH',
        'stock_iva': 'STOCK_IVA_SODIMAC',
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


class SodimacB2BPortal(
        B2BPortalBase,
        SodimacB2BPortalSource,
        B2BWebFetcher):

    name = 'b2b-sodimac-portal'
    related_fetchers = [SodimacB2BSales]

    string_variables = {
        'username': 'B2B_USERNAME_SODIMAC',
        'repository_path': 'SOURCE_INT_PATH',
        'password': 'B2B_PASSWORD_SODIMAC',
        'empresa': 'B2B_EMPRESA_SODIMAC',
    }

    @classmethod
    def settings(cls):
        args = super().settings()
        args['b2b_empresa'] = os.environ.get(cls.string_variables['empresa'], '')
        return args
