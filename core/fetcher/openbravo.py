import calendar
import datetime
import os

from core.fetcher import DailyFetcher
from core.storages.onlycsv import OnlyCsvStorage
from core.sources.openbravo import OpenbravoSource
from core.utils import timestamp_to_wivo_format
from utils.logger import logger


class OpenbravoSales(
        OnlyCsvStorage,
        OpenbravoSource,
        DailyFetcher):

    name = 'openbravo-sales'
    HISTORICAL_CACHE_ENABLE = True

    principal_url = 'https://openbravo.superdona.com.mx/openbravo'

    ORDER_URL = principal_url + "/ws/dal/Order"
    PRODUCT_URL = principal_url + "/ws/dal/Product/{}?_selectedProperties=productCategory"
    ALLPRODUCT_URL = principal_url + \
        "/ws/dal/Product?_selectedProperties=productCategory"

    @classmethod
    def settings(cls):
        openbravo_user = os.environ['OPENBRAVO_USER']
        openbravo_password = os.environ['OPENBRAVO_PASS']
        storage_path = os.environ['STORAGE_PATH']
        timezone_name = os.environ['TIMEZONE_NAME']
        return {
            'openbravo_user': openbravo_user,
            'openbravo_password': openbravo_password,
            'storage_path': storage_path,
            'timezone_name': timezone_name
        }

    def parse_data(self, connector, **kwargs):
        return self.parse_sales(connector)

    def parse_sales(self, connector):
        string_date = self.actual_date_tz.strftime('%Y-%m-%d')
        content = connector.execute_request(
            self.ORDER_URL,
            string_date
        )
        sales = []
        salesunit = []
        pc_cache = {}

        if 'Order' in content['ob:Openbravo']:
            logger.info("caching products...")
            prod_cat = connector.get_data(self.ALLPRODUCT_URL,)
            for product in prod_cat['ob:Openbravo']['Product']:
                pc_cache[product['@id']] = (
                    product['productCategory']['@id'], product['productCategory']['@identifier'])
            for countOrder, rowOrder in enumerate(
                    content['ob:Openbravo']['Order']):
                try:
                    oneOrder = False
                    if isinstance(rowOrder, str):
                        rowOrder = content['ob:Openbravo']['Order']
                        oneOrder = True
                    if isinstance(rowOrder['orderLineList']['OrderLine'], list):
                        orders = rowOrder['orderLineList']['OrderLine']
                    else:
                        orders = []
                        orders.append(rowOrder['orderLineList']['OrderLine'])
                    for countOrderLine, rowOrderLine in enumerate(orders):
                        logger.info(
                            'Processing {} Line {}'.format(
                                countOrder + 1,
                                countOrderLine + 1))
                        if not isinstance(rowOrderLine, str):
                            dt = datetime.datetime.strptime(
                                str(rowOrderLine['creationDate']['#text']),
                                '%Y-%m-%dT%H:%M:%S.%fZ')
                            date_time = calendar.timegm(dt.utctimetuple())
                            string_datetime = timestamp_to_wivo_format(date_time)

                            product_id = rowOrderLine['product']['@id']
                            product_name = rowOrderLine['product']['@identifier']

                            if product_id in pc_cache:
                                category_id, category_name = pc_cache[product_id]
                            else:
                                try:
                                    ccontent = connector.get_data(
                                        self.PRODUCT_URL.format(product_id),
                                    )
                                    pc = ccontent['ob:Openbravo']['Product']
                                    pc = pc['productCategory']
                                    category_id = pc['@id']
                                    category_name = pc['@identifier']
                                    pc_cache[product_id] = (
                                        category_id, category_name)
                                except BaseException:
                                    logger.error("Error al solicitar categoría")
                                    pc_cache[product_id] = ('Null', 'Null')
                                    category_id, category_name = pc_cache[product_id]

                            salesunit.append({
                                'ticket_id': rowOrder['documentNo'],
                                'store_id': rowOrder['warehouse']['@id'],
                                'store_name': rowOrder['warehouse']['@identifier'],
                                'product_id': product_id,
                                'product_name': product_name,
                                'client_id': rowOrder['client']['@id'],
                                'client_name': rowOrder['client']['@identifier'],
                                'category_id': category_id,
                                'category_name': category_name,
                                'seller_id': rowOrder['createdBy']['@id'],
                                'seller_name': rowOrder['createdBy']['@identifier'],
                                'businesspartner_id': rowOrder['businessPartner']['@id'],
                                'businesspartner_name': rowOrder['businessPartner']['@identifier'],
                                'obposApplication_id': rowOrder['obposApplications']['@id'],
                                'obposApplication_name': rowOrder['obposApplications']['@identifier'],
                                'datetime': string_datetime,
                                'value': rowOrderLine['orderedQuantity'],
                            })

                            sales.append({
                                'ticket_id': rowOrder['documentNo'],
                                'store_id': rowOrder['warehouse']['@id'],
                                'store_name': rowOrder['warehouse']['@identifier'],
                                'product_id': product_id,
                                'product_name': product_name,
                                'client_id': rowOrder['client']['@id'],
                                'client_name': rowOrder['client']['@identifier'],
                                'category_id': category_id,
                                'category_name': category_name,
                                'seller_id': rowOrder['createdBy']['@id'],
                                'seller_name': rowOrder['createdBy']['@identifier'],
                                'businesspartner_id': rowOrder['businessPartner']['@id'],
                                'businesspartner_name': rowOrder['businessPartner']['@identifier'],
                                'obposApplication_id': rowOrder['obposApplications']['@id'],
                                'obposApplication_name': rowOrder['obposApplications']['@identifier'],
                                'datetime': string_datetime,
                                'value': rowOrderLine['lineNetAmount'],
                            })
                    if oneOrder:
                        break
                except Exception as e:
                    logger.error("Error {}".format(e))

        return [{'metric': 'sale', 'records': sales},
                {'metric': 'salesunit', 'records': salesunit}, ]
