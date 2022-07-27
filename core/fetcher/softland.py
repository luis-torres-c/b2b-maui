import calendar
import datetime
import os
from collections import defaultdict

from core.fetcher import DailyFetcher
from core.storages.base import GenericMetricsObjectsCSVStorage
from core.sources.sqlserver import SQLServerSource
from core.utils import timestamp_to_wivo_format, datetime_to_wivo_format
from utils.logger import logger


class SoftlandSalesFetcher(
        GenericMetricsObjectsCSVStorage,
        SQLServerSource,
        DailyFetcher):

    name = 'softland-softland-sales'
    HISTORICAL_CACHE_ENABLE = True

    SALES_QUERY = '''
    SELECT softland.iw_gmovi.Tipo,
    softland.iw_gsaen.Folio,
    softland.iw_gmovi.NroInt,
    CodProd,
    DetProd,
    softland.iw_gmovi.CodBode,
    softland.iw_gmovi.Fecha,
    TotLinea,
    CantFactUVta,
    softland.iw_gsaen.CodVendedor,
    softland.iw_gsaen.Estado,
    softland.iw_gsaen.Descto01,
    softland.iw_gsaen.Descto02,
    softland.iw_gsaen.Descto03,
    softland.iw_gsaen.Descto04,
    softland.iw_gsaen.Descto05,
    softland.iw_gsaen.FecHoraCreacion
    FROM softland.iw_gmovi
    INNER JOIN softland.iw_gsaen ON
    softland.iw_gmovi.NroInt = softland.iw_gsaen.NroInt AND
    softland.iw_gmovi.Tipo = softland.iw_gsaen.Tipo
    WHERE softland.iw_gmovi.Fecha BETWEEN %s AND %s
    AND softland.iw_gmovi.Tipo=%s
    AND softland.iw_gsaen.Estado != 'P'
    '''

    VENDEDORES_QUERY = '''SELECT VenCod, VenDes FROM softland.cwtvend'''

    NC_QUERY = '''
    SELECT NroInt, CodBode, NetoAfecto, AuxDocfec, Folio, CodVendedor,
    softland.cwtvend.VenDes
    FROM softland.iw_gsaen
    INNER JOIN softland.cwtvend ON
    softland.iw_gsaen.CodVendedor=softland.cwtvend.VenCod
    WHERE AuxDocfec BETWEEN %s AND %s
    AND Tipo = 'N'
    '''

    @classmethod
    def settings(cls):
        sqlserver_host = os.environ['SQLSERVER_HOST']
        sqlserver_user = os.environ['SQLSERVER_USER']
        sqlserver_password = os.environ['SQLSERVER_PASSWORD']
        storage_path = os.environ['STORAGE_PATH']
        cls.sqlserver_database = os.environ['SQLSERVER_DATABASE']
        timezone_name = os.environ['TIMEZONE_NAME']
        return {
            'sqlserver_host': sqlserver_host,
            'sqlserver_user': sqlserver_user,
            'sqlserver_password': sqlserver_password,
            'storage_path': storage_path,
            'sqlserver_database': cls.sqlserver_database,
            'timezone_name': timezone_name
        }

    @classmethod
    def get_database(cls):
        return cls.sqlserver_database

    def parse_data(self, connector, **kwargs):
        return self.parse_sales(connector)

    def parse_sales(self, connector):
        string_date = self.actual_date_tz.strftime('%Y%m%d')
        types = ('B', 'F')
        sales = []
        salesunit = []
        products = []
        seller = []
        for t in types:
            cursor = connector.execute_query(
                self.SALES_QUERY,
                params=(string_date, string_date, t),
                database=self.get_database(),
            )

            discount_cache = {}
            for count, row in enumerate(cursor):
                ticket_id = '{}-{}'.format(row['Tipo'], row['Folio'])
                logger.debug('Processing {} Ticket {}'.format(count + 1, ticket_id))
                fecha_hora = row['FecHoraCreacion']
                if fecha_hora:
                    try:
                        dt = datetime.datetime.strptime(
                            str(fecha_hora), '%Y-%m-%d %H:%M:%S.%f')
                    except BaseException:
                        dt = datetime.datetime.strptime(
                            str(fecha_hora), '%Y-%m-%d %H:%M:%S')
                else:
                    logger.warning("Ticket {} whitout time creation".format(ticket_id))
                    dt = datetime.datetime.combine(self.actual_date_tz, datetime.datetime.min.time())

                date_time = calendar.timegm(dt.utctimetuple())
                string_datetime = timestamp_to_wivo_format(date_time)
                if (t == 'B' and row['Estado'] != 'N') or (
                        t == 'F' and row['Estado'] != 'N'):
                    seller_id = row['CodVendedor'] or 'emptySeller'

                    salesunit.append({
                        'ticket_id': ticket_id,
                        'store_id': row['CodBode'].strip(),
                        'product_id': row['CodProd'].strip(),
                        'seller_id': seller_id,
                        'datetime': string_datetime,
                        'value': row['CantFactUVta'],
                    })

                    sales.append({
                        'ticket_id': ticket_id,
                        'store_id': row['CodBode'].strip(),
                        'product_id': row['CodProd'].strip(),
                        'datetime': string_datetime,
                        'seller_id': seller_id,
                        'value': row['TotLinea'],
                    })

                    products.append({
                        'product_name': row['DetProd'].strip() or 'Producto sin nombre',
                        'product_id': row['CodProd'].strip() or 'Producto sin ID',
                    })

                    discount = row['Descto01'] + row['Descto02'] + \
                        row['Descto03'] + row['Descto04'] + row['Descto05']

                    if discount > 0 and ticket_id not in discount_cache:
                        discount_cache[ticket_id] = discount

                        sales.append({
                            'ticket_id': ticket_id,
                            'store_id': row['CodBode'].strip(),
                            'product_id': 'wivo_DES',
                            'datetime': string_datetime,
                            'seller_id': seller_id,
                            'value': discount * -1,
                        })

        cursor = connector.execute_query(
            self.NC_QUERY,
            params=(string_date, string_date),
            database=self.get_database(),
        )
        for count, row in enumerate(cursor):
            logger.info(
                'Processing {} Ticket NC {}'.format(
                    count + 1, row['Folio']))
            ticket_id = '{}-{}'.format('NC', row['Folio'])
            fecha_hora = row['AuxDocfec']
            dt = datetime.datetime.strptime(
                str(fecha_hora), '%Y-%m-%d %H:%M:%S')
            date_time = calendar.timegm(dt.utctimetuple())
            string_datetime = timestamp_to_wivo_format(date_time)
            seller_id = row['CodVendedor'] or 'emptySeller'

            sales.append({
                'ticket_id': ticket_id,
                'store_id': row['CodBode'].strip(),
                'product_id': 'wivo_PD',
                'datetime': string_datetime,
                'seller_id': seller_id,
                'value': row['NetoAfecto'],
            })

        cursor = connector.execute_query(
            self.VENDEDORES_QUERY,
            params=(),
            database=self.get_database(),
        )
        for count, row in enumerate(cursor):
            seller.append({
                'seller_id': row['VenCod'],
                'seller_name': row['VenDes'],
            })

        return [{'metric': 'sale', 'records': sales},
                {'metric': 'salesunit', 'records': salesunit},
                {'object': 'product', 'records': products},
                {'object': 'seller', 'records': seller}, ]


class SoftlandStockFetcher(
        GenericMetricsObjectsCSVStorage,
        SQLServerSource,
        DailyFetcher):

    name = 'softland-softland-stock'
    HISTORICAL_CACHE_ENABLE = True

    STOCK_QUERY = '''
        EXEC
        softland.iw_pdblInformeStock
        @FiltroProd=0, @Serie='1', @Partida='3',
        @CodBode=%s, @IndProductos='1', @fecha=%s
    '''

    BODE_QUERY = 'SELECT CodBode FROM softland.iw_tbode'

    EXCLUDE_CATEGORY = []

    CHANGE_NAME_CATEGORY = []

    @classmethod
    def settings(cls):
        sqlserver_host = os.environ['SQLSERVER_HOST']
        sqlserver_user = os.environ['SQLSERVER_USER']
        sqlserver_password = os.environ['SQLSERVER_PASSWORD']
        storage_path = os.environ['STORAGE_PATH']
        cls.sqlserver_database = os.environ['SQLSERVER_DATABASE']
        timezone_name = os.environ['TIMEZONE_NAME']
        return {
            'sqlserver_host': sqlserver_host,
            'sqlserver_user': sqlserver_user,
            'sqlserver_password': sqlserver_password,
            'storage_path': storage_path,
            'timezone_name': timezone_name
        }

    @classmethod
    def get_database(cls):
        return cls.sqlserver_database

    def parse_data(self, connector, **kwargs):
        string_date = self.actual_date_tz.strftime('%d/%m/%Y')

        cursor = connector.execute_query(
            self.BODE_QUERY,
            database=self.get_database()
        )
        stores_id = []
        for count, row in enumerate(cursor):
            stores_id.append(row['CodBode'])

        stockunit = []
        stock = []
        product = []
        store = []
        brand = []
        category = []
        r_brand_product = []
        r_category_product = []

        product_dict = defaultdict(dict)
        store_dict = defaultdict(dict)
        brand_dict = defaultdict(dict)
        category_dict = defaultdict(dict)

        for count_stores, store_id in enumerate(stores_id):
            cursor = connector.execute_query(
                self.STOCK_QUERY,
                params=(store_id, string_date),
                database=self.get_database()
            )
            logger.info('Processing {}'.format(count_stores + 1))
            for count, row in enumerate(cursor):
                product_name = row['DesProd'] or 'Producto sin nombre'
                product_id = row['CodProd'] or 'Producto sin ID'
                store_name = row['DesBode'] or 'Tienda sin nombre'
                value = row['StockTotal']
                cost = row['CostoTotal']
                category_id = row['CodSubGr'] or 'Sin categoria'
                category_name = row['DesSubGr'] or 'Sin categoria'
                brand_name = row['DesGrupo'] or 'Sin marca'
                brand_id = row['CodGrupo'] or 'Sin marca'

                if brand_id not in self.EXCLUDE_CATEGORY:

                    if brand_id in self.CHANGE_NAME_CATEGORY:
                        brand_name = brand_id

                    date_ = datetime.datetime.strptime(
                        string_date, '%d/%m/%Y').date()
                    date_time = datetime.datetime.combine(
                        date_, datetime.time.min)

                    stockunit.append({
                        'datetime': datetime_to_wivo_format(date_time),
                        'value': value,
                        'store_id': store_id,
                        'product_id': product_id
                    })
                    stock.append({
                        'datetime': datetime_to_wivo_format(date_time),
                        'value': cost,
                        'store_id': store_id,
                        'product_id': product_id
                    })

                    if product_id not in product_dict.keys():
                        product_dict[product_id] = product_name
                        product.append({
                            'product_id': product_id,
                            'product_name': product_name
                        })

                    if category_id and category_id not in category_dict.keys():
                        category_dict[category_id] = category_name
                        category.append({
                            'category_id': category_id,
                            'category_name': category_name
                        })

                    if brand_id and brand_id not in brand_dict.keys():
                        brand_dict[brand_id] = brand_name
                        brand.append({
                            'brand_id': brand_id,
                            'brand_name': brand_name
                        })

                    if store_id not in store_dict.keys():
                        store_dict[store_id] = store_name
                        store.append({
                            'store_id': store_id,
                            'store_name': store_name
                        })
                    rbp = {'brand_id': brand_id, 'product_id': product_id}
                    if rbp not in r_brand_product:
                        r_brand_product.append(rbp)

                    rcp = {
                        'category_id': category_id,
                        'product_id': product_id}
                    if rcp not in r_category_product:
                        r_category_product.append(rcp)

        product.append({
            'product_id': 'wivo_PD',
            'product_name': 'PRODUCTO DESCONTADO',
        })
        product.append({
            'product_id': 'wivo_DES',
            'product_name': 'PRODUCTO DESCUENTO',
        })

        return [{'metric': 'stockunit', 'date': date_time, 'records': stockunit},
                {'metric': 'stock', 'date': date_time, 'records': stock},
                {'object': 'product', 'records': product},
                {'object': 'category', 'records': category},
                {'object': 'brand', 'records': brand},
                {'object': 'store', 'records': store},
                {'relation': 'brands->products', 'records': r_brand_product},
                {'relation': 'categories->products',
                    'records': r_category_product}]
