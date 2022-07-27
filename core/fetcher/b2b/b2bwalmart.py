import os

from core.sources.b2b.b2bwalmart import WalmartB2BPortalSource, WalmartB2BFileSource, WalmartB2BFileStockSource

from core.storages.onlycsv import OnlyCsvStorage
from core.utils import datetime_to_wivo_format, create_id
from core.fetcher.b2b.base import B2BPortalBase, B2BFile
from core.fetcher.base import B2BWebFetcher, DailyFetcher


class WalmartChileFile(B2BFile, WalmartB2BFileSource, OnlyCsvStorage, DailyFetcher):

    name = 'b2b-walmartchile-files'

    string_variables = {
        'username': 'B2B_USERNAME_WALMART',
        'repository_path': 'SOURCE_INT_PATH',
    }

    DEFAULT_CHAIN_ID = 'walmartCL'
    IVA = 1.19
    PORTAL = 'Walmart'

    date_format = '%Y-%m-%d %H:%M:%S 00:00'

    # Another weird thing from wallmart. Values that represent money has to be multiplied by 100
    # to get the real value.
    MAGIC_NUMBER = 100

    @classmethod
    def settings(cls):
        variables = super().settings()
        repository_path = os.environ.get(cls.string_variables['repository_path'], '')
        variables['repository_path'] = repository_path

        return variables

    def base_data(self, row, date_time):
        b2bproduct_id = row['Núm del Artículo Principal']
        product_name = row['Desc  Artículo Principal']
        self.chain_id = self.DEFAULT_CHAIN_ID
        chain_name = 'Walmart'
        store_name = row['Nombre de Tienda']
        store_id = self.chain_id + str(int(float(row['Núm de Tienda'])))
        codmodel = row['Número de Artículo de Sistema Legado']
        brand_id = self.chain_id + str(row['ID de Marca'])
        brand_name = row['Descripción de Marca']
        category = row['Descripción Categoría del Dpto']

        base_data = {
            self.mapping_column_name('source_id'): self.chain_id,
            self.mapping_column_name('source_name'): chain_name,
            self.mapping_column_name('store_id'): store_id,
            self.mapping_column_name('store_name'): store_name,
            self.mapping_column_name('product_id'): self.chain_id + b2bproduct_id,
            self.mapping_column_name('product_name'): product_name,
            self.mapping_column_name('brand_id'): brand_id,
            self.mapping_column_name('brand_name'): brand_name,
            self.mapping_column_name('category_id'): self.chain_id + create_id(category),
            self.mapping_column_name('category_name'): category,
            self.mapping_column_name('codstore_id'): store_id,
            self.mapping_column_name('codstore_name'): str(int(float(row['Núm de Tienda']))),
            self.mapping_column_name('codproduct_id'): self.chain_id + b2bproduct_id,
            self.mapping_column_name('codproduct_name'): b2bproduct_id,
            self.mapping_column_name('codmodel_id'): self.chain_id + create_id(codmodel),
            self.mapping_column_name('codmodel_name'): codmodel,
            'datetime': datetime_to_wivo_format(date_time)}

        return base_data

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):

        net_values = kwargs['net_values']
        allow_sales_zero = bool(kwargs.get('allow_sales_zero', True))

        sale = row['Venta POS']
        sale = sale * self.MAGIC_NUMBER
        if not net_values:
            sale = sale * self.IVA

        saleunits_val = float(row['Cnt POS'])
        saleunits_value = {
            'value': saleunits_val
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


class WalmartChileStockFile(WalmartB2BFileStockSource, WalmartChileFile):

    name = 'b2b-walmartchile-stock-files'

    def metric_values(self, row, metrics, date_time, base_data, **kwargs):

        net_values = kwargs['net_values']

        stockunits_value = {
            'value': row['Cantidad Actual en Existentes de la tienda']
        }
        if net_values:
            stock_value = row['Costo Actual en Existentes de la tienda'] * self.MAGIC_NUMBER
            stock = stock_value if stock_value != 0 else 0
        else:
            stock_value = row['Costo Actual en Existentes de la tienda'] * self.MAGIC_NUMBER * self.IVA
            stock = stock_value if stock_value != 0 else 0
        stocks_value = {
            'value': stock
        }
        self.append(metrics['stockunit'], date_time, {**base_data, **stockunits_value})
        self.append(metrics['stock'], date_time, {**base_data, **stocks_value})


class WalmartChileWeb(B2BPortalBase, WalmartB2BPortalSource, B2BWebFetcher):

    name = 'b2b-walmartchile-portal'
    related_fetchers = [WalmartChileFile]
