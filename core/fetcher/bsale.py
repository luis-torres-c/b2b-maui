import copy
import datetime

from core.fetcher import DailyFetcher
from core.sources.bsale import BSaleSource
from core.storages.base import GenericMetricsObjectsCSVStorage
from core.storages.onlycsv import OnlyCsvStorage
from core.utils import (
    apply_prefix,
    create_id,
    default_empty_value,
    timestamp_to_wivo_format,
    datetime_to_wivo_format_with_tz,
    datetime_to_wivo_format,
)
from utils.logger import logger


# TODO move it to utils.py
def _is_same_day_for_timestamps(timestamp1, timestamp2):
    dt1 = datetime.datetime.utcfromtimestamp(timestamp1)
    dt2 = datetime.datetime.utcfromtimestamp(timestamp2)
    return dt1.date() == dt2.date()


class BsaleDocuments(OnlyCsvStorage, BSaleSource, DailyFetcher):

    name = 'bsale-documents'

    EXPANDED_DOCUMENTS_FIELDS = (
        'variant',
        'office',
        'product',
        'details',
        'document_type',
        'client',
        'sellers',
    )

    EXPANDED_VARIANT_FIELDS = (
        'product',
        'product_type',
        'costs'
    )

    MAPPING_METRIC_NAMES = {}

    BODEGAS = []

    USE_GROSS_VALUE = True
    USE_DOCUMENT_ID_AS_TICKET_ID = False

    @classmethod
    def settings(cls):
        import os
        tokens = os.environ['TOKENS'].split(';')
        storage_path = os.environ['STORAGE_PATH']
        document_types_to_add = os.environ['DOCUMENT_TYPES_TO_ADD'].split(';')
        document_types_to_add = list(map(int, document_types_to_add))
        document_types_to_subtract = os.environ['DOCUMENT_TYPES_TO_SUBTRACT'].split(';')
        document_types_to_subtract = list(map(int, document_types_to_subtract))
        timezone_name = os.environ['TIMEZONE_NAME']
        return {
            'tokens': tokens,
            'storage_path': storage_path,
            'document_types_to_add': document_types_to_add,
            'document_types_to_subtract': document_types_to_subtract,
            'timezone_name': timezone_name,
        }

    def _mapping_metric_name(self, metric_name):
        if metric_name in self.MAPPING_METRIC_NAMES:
            return self.MAPPING_METRIC_NAMES[metric_name]
        return metric_name

    def parse_data(self, connector, **kwargs):
        timezone_name = kwargs['timezone_name']
        doc_types_to_add = kwargs['document_types_to_add']
        doc_types_to_subtract = kwargs['document_types_to_subtract']
        valid_document_types = list(map(lambda i: int(i), doc_types_to_add + doc_types_to_subtract))
        # TODO externalize
        default_empty_id = ''
        empty_product_label = ''
        empty_variant_label = ''
        empty_variant_code = ''
        empty_category_label = ''
        empty_seller_label = ''
        empty_client_label = ''
        empty_pricelist_label = ''
        classification = {
            0: 'Producto',
            1: 'Servicio',
            3: 'Pack o Promocion',
        }
        empty_classification = {'id': default_empty_id, 'name': ''}

        sales_records = []
        sales_units_records = []
        categories_products_relation_records = []
        sellin_records = []
        sellinunits_records = []
        unique_relation_categories_products = set()
        variant_detail_data_cache = {}
        for data in connector.get_documents(expanded_fields=self.EXPANDED_DOCUMENTS_FIELDS):
            logger.info('Items Total {} offset {}'.format(data['count'], data['offset']))
            for document in data['items']:
                document_id = document['id']
                document_number = document['number']
                if self.USE_DOCUMENT_ID_AS_TICKET_ID:
                    ticket_id = document_id
                else:
                    ticket_id = document_number
                if not document['document_type']['id'] in valid_document_types:
                    logger.warning('Not valid Type ID {} {} - Skipping Document ID {}'.format(
                        document['document_type']['id'], document['document_type']['name'], document_id))
                    continue

                document_type_id = document['document_type']['id']
                document_type_name = document['document_type']['name']

                logger.info('Processing Document ID {} Number {}'.format(document_id, document_number))
                store_id = document['office']['id']
                store_name = document['office']['name']

                client_id = ''
                client_name = ''
                client_code = ''
                client_company = ''
                pricelist_id = ''
                pricelist_name = ''
                if 'client' in document:
                    client_code = document['client']['code']
                    client_id = document['client']['id']
                    client_company = document['client']['company']
                    first_name_client = document['client']['firstName'] or ''
                    last_name_client = document['client']['lastName'] or ''
                    first_name_client = first_name_client.strip()
                    last_name_client = last_name_client.strip()

                    if first_name_client and last_name_client:

                        client_name = '{} {}'.format(
                            first_name_client.strip(),
                            last_name_client.strip(),
                        )
                    elif first_name_client:
                        client_name = first_name_client
                    elif last_name_client:
                        client_name = last_name_client
                    else:
                        client_name = ''

                    if 'price_list' in document['client']:
                        price_list_url = document['client']['price_list']['href']
                        price_list_data = connector.bsale_request(url=price_list_url)
                        pricelist_id = price_list_data['id']
                        pricelist_name = price_list_data['name']

                seller_id = document['sellers']['items'][0]['id']
                first_name_seller = document['sellers']['items'][0]['firstName'].strip()
                last_name_seller = document['sellers']['items'][0]['lastName'].strip()
                seller_name = '{} {}'.format(
                    first_name_seller,
                    last_name_seller
                )

                factor = 1
                ts_datetime = document['generationDate']
                if document['document_type']['id'] in doc_types_to_subtract:
                    logger.debug(
                        'Set value as negative and using emissionDate as date reference by document type {}'.format(
                            document['document_type']['name']))
                    ts_datetime = document['emissionDate']
                    wivo_string_datetime = timestamp_to_wivo_format(ts_datetime)
                    factor = -1
                else:
                    if not _is_same_day_for_timestamps(ts_datetime, connector.timestamp_start):
                        logger.info(
                            'Using end of the day datetime instead of generationDate {}'.format(
                                timestamp_to_wivo_format(ts_datetime))
                        )
                        ts_datetime = connector.timestamp_end
                    dt_utc = datetime.datetime.utcfromtimestamp(ts_datetime)
                    wivo_string_datetime = datetime_to_wivo_format_with_tz(dt_utc, timezone_name)

                if not document['details']['items']:
                    logger.warning('It doesnt have details doc number {}'.format(ticket_id))

                detail_items = document['details']['items']
                details = document['details']
                while 'next' in details:
                    details = connector.bsale_request(url=details['next'])
                    detail_items.extend(details['items'])

                for detail in detail_items:
                    logger.info('Processing sale detail item {}'.format(
                        detail['variant']['description'] or detail['variant']['id']))

                    if self.USE_GROSS_VALUE:
                        # Using gross value
                        sale_value = int(detail['totalAmount'])
                    else:
                        # Using net value
                        sale_value = int(detail['netAmount'])

                    quantity = detail['quantity']

                    variant_detail_url = detail['variant']['href']
                    if variant_detail_url in variant_detail_data_cache:
                        # Cache
                        logger.debug(f'Hit Cache - Variant for {variant_detail_url}')
                        variant_detail_data = variant_detail_data_cache[variant_detail_url]
                    else:
                        variant_detail_data = connector.bsale_request(
                            url=variant_detail_url, expanded_fields=self.EXPANDED_VARIANT_FIELDS)
                        variant_detail_data_cache[variant_detail_url] = variant_detail_data

                    variant_id = variant_detail_data['id']
                    variant_name = variant_detail_data['description']
                    variant_code = variant_detail_data['code']
                    # Temporal fix until chigurh supports empty name in objects when id exists.
                    if variant_id and isinstance(variant_name, str) and not variant_name.strip():
                        variant_name = 'Sin Nombre ({})'.format(variant_id)
                    classification_id = variant_detail_data['product'].get('classification', '')
                    if classification_id is None:
                        classification_id = empty_classification['id']
                    classification_name = classification.get(classification_id) or empty_classification['name']
                    product_id = variant_detail_data['product']['id']
                    product_name = variant_detail_data['product']['name']
                    # Temporal fix until chigurh supports empty name in objects when id exists.
                    if product_id and isinstance(product_name, str) and not product_name.strip():
                        product_name = 'Sin Nombre ({})'.format(product_id)
                    category_id = variant_detail_data['product']['product_type']['id']
                    category_name = variant_detail_data['product']['product_type']['name']

                    if kwargs['is_multi_tokens']:
                        prefix = connector.token[-7:]
                        store_id = apply_prefix(prefix, store_id)
                        ticket_id = apply_prefix(prefix, ticket_id)
                        variant_id = create_id(variant_name)
                        client_id = apply_prefix(prefix, client_id)
                        seller_id = apply_prefix(prefix, seller_id)
                        product_id = create_id(product_name)
                        category_id = apply_prefix(prefix, category_id)

                    if category_id and product_id:
                        relation_id = '{}-{}'.format(category_id, product_id)
                        # Saving unique relation
                        if relation_id not in unique_relation_categories_products:
                            category_product_record = {
                                'category_id': category_id,
                                'product_id': product_id,
                            }
                            unique_relation_categories_products.add(relation_id)
                            categories_products_relation_records.append(category_product_record)

                    sale_record = {
                        'ticket_id': ticket_id,
                        'store_id': store_id,
                        'store_name': store_name,
                        'variant_id': variant_id or default_empty_id,
                        'variant_name': variant_name or empty_variant_label,
                        'variant_code': variant_code or empty_variant_code,
                        'client_id': client_id or default_empty_id,
                        'client_name': client_name or empty_client_label,
                        'client_code': client_code or default_empty_id,
                        'client_company': client_company or empty_client_label,
                        'seller_id': seller_id or default_empty_id,
                        'seller_name': seller_name or empty_seller_label,
                        'product_id': product_id or default_empty_id,
                        'product_name': product_name or empty_product_label,
                        'category_id': category_id or default_empty_id,
                        'category_name': category_name or empty_category_label,
                        'classification_id': classification_id,
                        'classification_name': classification_name,
                        'documenttype_id': document_type_id,
                        'documenttype_name': document_type_name,
                        'pricelist_id': pricelist_id or default_empty_id,
                        'pricelist_name': pricelist_name or empty_pricelist_label,
                        'datetime': wivo_string_datetime,
                    }
                    sale_record.update({'value': sale_value * factor})

                    sale_unit_record = copy.copy(sale_record)
                    sale_unit_record.update({'value': quantity * factor})

                    if store_id in self.BODEGAS:
                        sellin_records.append(sale_record)
                        sellinunits_records.append(sale_unit_record)
                    else:
                        sales_records.append(sale_record)
                        sales_units_records.append(sale_unit_record)
        ret = [
            {'metric': self._mapping_metric_name('sale'), 'records': sales_records},
            {'metric': self._mapping_metric_name('salesunit'), 'records': sales_units_records},
            {'relation': 'categories->products', 'records': categories_products_relation_records}
        ]
        if sellin_records and sellinunits_records:
            ret.append({'metric': self._mapping_metric_name('sellin'), 'records': sellin_records})
            ret.append({'metric': self._mapping_metric_name('sellinunit'), 'records': sellinunits_records})
        return ret


class BsaleStocks(OnlyCsvStorage, BSaleSource, DailyFetcher):

    name = 'bsale-stocks'

    # This Fetcher doesnt need a consolidation process
    PROCESS_BY_RANGE_DAYS_ENABLE = False

    EXPANDED_FIELDS = [
        'variant',
        'office',
        'product',
        'costs'
    ]
    LIMIT = 200

    MAPPING_METRIC_NAMES = {}

    BODEGAS = []

    @classmethod
    def settings(cls):
        import os
        tokens = os.environ['TOKENS'].split(';')
        storage_path = os.environ['STORAGE_PATH']
        timezone_name = os.environ['TIMEZONE_NAME']
        return {
            'tokens': tokens,
            'storage_path': storage_path,
            'timezone_name': timezone_name,
        }

    def _mapping_metric_name(self, metric_name):
        if metric_name in self.MAPPING_METRIC_NAMES:
            return self.MAPPING_METRIC_NAMES[metric_name]
        return metric_name

    def parse_data(self, connector, **kwargs):
        prefix = connector.token[-7:]
        list_stocks = []
        list_stocks_units = []
        list_sellinstockunits = []
        list_sellinstocks = []
        count = 0
        default_empty_id = ''
        empty_product_label = ''
        empty_variant_label = ''
        string_datetime = datetime_to_wivo_format(self.actual_datetime_tz)
        product_type_data_cache = {}
        for data in connector.get_stocks(expanded_fields=self.EXPANDED_FIELDS, limit=self.LIMIT):
            logger.info('Items Total {} offset {}'.format(data['count'], data['offset']))
            for item in data['items']:
                count += 1
                logger.info('Processing {} Stock ID {}'.format(count, item['id']))
                product_name = item['variant']['product']['name']
                variant_name = item['variant']['description']
                variant_code = item['variant']['code']
                store_id = item['office']['id']
                store_name = item['office']['name']
                variant_id = item['variant']['id']
                # Temporal fix until chigurh supports empty name in objects when id exists.
                if variant_id and isinstance(variant_name, str) and not variant_name.strip():
                    variant_name = 'Sin Nombre ({})'.format(variant_id)
                product_id = item['variant']['product']['id']
                # Temporal fix until chigurh supports empty name in objects when id exists.
                if product_id and isinstance(product_name, str) and not product_name.strip():
                    product_name = 'Sin Nombre ({})'.format(product_id)
                product_type_url = item['variant']['product']['product_type']['href']
                if product_type_url in product_type_data_cache:
                    # cache
                    logger.debug(f'Hit Cache - Request for {product_type_url}')
                    product_type_data = product_type_data_cache[product_type_url]
                else:
                    product_type_data = connector.bsale_request(url=product_type_url)
                    product_type_data_cache[product_type_url] = product_type_data

                category_id = product_type_data['id']
                category_name = product_type_data['name']
                quantity_available = item['quantityAvailable']
                if kwargs['is_multi_tokens']:
                    prefix = connector.token[-7:]
                    store_id = apply_prefix(prefix, store_id)
                    variant_id = create_id(variant_name)
                    product_id = create_id(product_name)
                cost_average = item['variant']['costs']['averageCost']
                stocks_units = {
                    'product_id': product_id or default_empty_id,
                    'product_name': product_name or empty_product_label,
                    'variant_id': variant_id or default_empty_id,
                    'variant_name': variant_name or empty_variant_label,
                    'variant_code': variant_code,
                    'store_id': store_id,
                    'store_name': store_name,
                    'category_id': category_id,
                    'category_name': category_name,
                    'datetime': string_datetime,
                    'value': quantity_available,
                }

                stocks = stocks_units.copy()
                stocks['value'] = quantity_available * float(cost_average)
                if store_id not in self.BODEGAS:
                    list_stocks_units.append(stocks_units)
                    list_stocks.append(stocks)
                else:
                    list_sellinstockunits.append(stocks_units)
                    list_sellinstocks.append(stocks)
        ret = [
            {'metric': self._mapping_metric_name('stock'), 'records': list_stocks},
            {'metric': self._mapping_metric_name('stockunit'), 'records': list_stocks_units}
        ]
        if list_sellinstocks and list_sellinstockunits:
            ret.append({'metric': self._mapping_metric_name('sellinstock'), 'records': list_sellinstocks})
            ret.append({'metric': self._mapping_metric_name('sellinstockunit'), 'records': list_sellinstockunits})
        return ret


class BsalePayments(GenericMetricsObjectsCSVStorage, BSaleSource, DailyFetcher):

        name = 'bsale-payments'

        HOURS_DELAY = 8

        EXPANDED_PAYMENTS_FIELDS = (
            'variant',
            'office',
            'product',
            'payment_type',
            'details',
            'document',
            'document_type',
            'client',
            'sellers',
        )

        EXPANDED_VARIANT_FIELDS = (
            'product',
            'product_type',
            'costs'
        )

        MAPPING_METRIC_NAMES = {}

        USE_GROSS_UNIT_VALUE = True
        USE_DOCUMENT_ID_AS_TICKET_ID = False

        @classmethod
        def settings(cls):
            import os
            tokens = os.environ['TOKENS'].split(';')
            storage_path = os.environ['STORAGE_PATH']
            document_types_to_add = os.environ['DOCUMENT_TYPES_TO_ADD'].split(';')
            document_types_to_add = list(map(int, document_types_to_add))
            document_types_to_subtract = os.environ['DOCUMENT_TYPES_TO_SUBTRACT'].split(';')
            document_types_to_subtract = list(map(int, document_types_to_subtract))
            payment_types_to_subtract = os.environ['PAYMENT_TYPES_TO_SUBTRACT'].split(';')
            payment_types_to_subtract = list(map(int, payment_types_to_subtract))
            timezone_name = os.environ['TIMEZONE_NAME']
            return {
                'tokens': tokens,
                'storage_path': storage_path,
                'document_types_to_add': document_types_to_add,
                'document_types_to_subtract': document_types_to_subtract,
                'payment_types_to_subtract': payment_types_to_subtract,
                'timezone_name': timezone_name,
            }

        def _mapping_metric_name(self, metric_name):
            if metric_name in self.MAPPING_METRIC_NAMES:
                return self.MAPPING_METRIC_NAMES[metric_name]
            return metric_name

        def parse_data(self, connector, **kwargs):
            timezone_name = kwargs['timezone_name']
            doc_types_to_add = kwargs['document_types_to_add']
            doc_types_to_subtract = kwargs['document_types_to_subtract']
            valid_document_types = list(map(lambda i: int(i), doc_types_to_add + doc_types_to_subtract))
            # TODO externalize
            empty_product_label = 'Sin nombre'
            empty_variant_label = 'Sin Variante'
            empty_variant_code = 'Sin Code'
            empty_category_label = 'Sin categoría'
            empty_seller_label = 'Sin nombre'
            empty_client_label = 'Sin nombre'
            classification = {
                0: 'Producto',
                1: 'Servicio',
                3: 'Pack o Promocion',
            }
            empty_classification = {'id': default_empty_value(), 'name': 'Sin clasificacion'}

            payment_records = []
            costs_records = []
            costs_units_records = []
            prices_records = []
            for data in connector.get_payments(expanded_fields=self.EXPANDED_PAYMENTS_FIELDS):
                logger.info('Items Total {} offset {}'.format(data['count'], data['offset']))
                for item in data['items']:
                    document = item['document']
                    document_id = document['id']
                    document_number = document['number']
                    if self.USE_DOCUMENT_ID_AS_TICKET_ID:
                        ticket_id = document_id
                    else:
                        ticket_id = document_number
                    if not document['document_type']['id'] in valid_document_types:
                        logger.debug('Not valid Type ID {} {} - Skipping Document ID {}'.format(
                            document['document_type']['id'], document['document_type']['name'], document_id))
                        continue

                    document_type_id = document['document_type']['id']
                    document_type_name = document['document_type']['name']

                    payment_type_id = item['payment_type']['id']
                    payment_type_name = item['payment_type']['name']

                    payment_id = item['id']
                    payment_value = item['amount']

                    logger.info('Processing Payment ID {}'.format(payment_id))
                    store_id = document['office']['id']
                    store_name = document['office']['name']

                    client_id = ''
                    client_name = ''
                    if 'client' in item:
                        client_id = document['client']['id']
                        first_name_client = document['client']['firstName'].strip()
                        last_name_client = document['client']['lastName'].strip()
                        client_name = '{} {}'.format(
                            first_name_client,
                            last_name_client,
                        )
                    seller_id = document['sellers']['items'][0]['id']
                    first_name_seller = document['sellers']['items'][0]['firstName'].strip()
                    last_name_seller = document['sellers']['items'][0]['lastName'].strip()
                    seller_name = '{} {}'.format(
                        first_name_seller,
                        last_name_seller
                    )

                    factor = 1
                    ts_datetime = document['generationDate']
                    if document['document_type']['id'] in doc_types_to_subtract:
                        logger.debug('Set value as negative and using emissionDate as date reference by document type {}'.format(
                            document['document_type']['name']))
                        ts_datetime = item['emissionDate']
                        wivo_string_datetime = timestamp_to_wivo_format(ts_datetime)
                        factor = -1
                    else:
                        if not _is_same_day_for_timestamps(ts_datetime, connector.timestamp_start):
                            logger.debug(
                                'Using end of the day datetime instead of generationDate {}'.format(
                                    timestamp_to_wivo_format(ts_datetime))
                            )
                            ts_datetime = connector.timestamp_end
                        dt_utc = datetime.datetime.utcfromtimestamp(ts_datetime)
                        wivo_string_datetime = datetime_to_wivo_format_with_tz(dt_utc, timezone_name)

                    if not document['details']['items']:
                        logger.warning('It doesnt have details doc number {}'.format(ticket_id))

                    detail_items = document['details']['items']
                    details = document['details']
                    while 'next' in details:
                        details = connector.bsale_request(url=item['details']['next'])
                        detail_items.extend(details['items'])

                    for detail in detail_items:
                        logger.info('Processing sale detail item {}'.format(
                            detail['variant']['description'] or detail['variant']['id']))

                        if self.USE_GROSS_UNIT_VALUE:
                            # Using gross unit value
                            unit_price = detail['totalUnitValue']
                        else:
                            # Using net unit value
                            unit_price = detail['netlUnitValue']

                        quantity = detail['quantity']

                        variant_detail_url = detail['variant']['href']
                        variant_detail_data = connector.bsale_request(
                            url=variant_detail_url, expanded_fields=self.EXPANDED_VARIANT_FIELDS)
                        variant_id = variant_detail_data['id']
                        variant_name = variant_detail_data['description']
                        variant_code = variant_detail_data['code']
                        variant_average_cost = variant_detail_data['costs']['averageCost']
                        classification_id = variant_detail_data['product'].get('classification', '')
                        if classification_id is None:
                            classification_id = empty_classification['id']
                        classification_name = classification.get(classification_id) or empty_classification['name']
                        product_id = variant_detail_data['product']['id']
                        product_name = variant_detail_data['product']['name']
                        category_id = variant_detail_data['product']['product_type']['id']
                        category_name = variant_detail_data['product']['product_type']['name']

                        if kwargs['is_multi_tokens']:
                            prefix = connector.token[-7:]
                            store_id = apply_prefix(prefix, store_id)
                            ticket_id = apply_prefix(prefix, ticket_id)
                            variant_id = create_id(variant_name)
                            client_id = apply_prefix(prefix, client_id)
                            seller_id = apply_prefix(prefix, seller_id)
                            product_id = create_id(product_name)
                            category_id = apply_prefix(prefix, category_id)

                        payment_record = {
                            'ticket_id': ticket_id,
                            'store_id': store_id,
                            'store_name': store_name,
                            'variant_id': variant_id or default_empty_value(),
                            'variant_name': variant_name or empty_variant_label,
                            'variant_code': variant_code or empty_variant_code,
                            'client_id': client_id or default_empty_value(),
                            'client_name': client_name or empty_client_label,
                            'seller_id': seller_id or default_empty_value(),
                            'seller_name': seller_name or empty_seller_label,
                            'product_id': product_id or default_empty_value(),
                            'product_name': product_name or empty_product_label,
                            'category_id': category_id or default_empty_value(),
                            'category_name': category_name or empty_category_label,
                            'classification_id': classification_id,
                            'classification_name': classification_name,
                            'documenttype_id': document_type_id,
                            'documenttype_name': document_type_name,
                            'paymenttype_id': payment_type_id,
                            'paymenttype_name': payment_type_name,
                            'datetime': wivo_string_datetime,
                            'value': payment_value * factor
                        }

                        cost_unit_record = copy.copy(payment_record)
                        cost_unit_record.update({'value': quantity})

                        cost_record = copy.copy(payment_record)
                        cost_record.update({'value': variant_average_cost})

                        price_record = {
                            'store_id': store_id,
                            'store_name': store_name,
                            'variant_id': variant_id or default_empty_value(),
                            'variant_name': variant_name or empty_variant_label,
                            'variant_code': variant_code or empty_variant_code,
                            'product_id': product_id or default_empty_value(),
                            'product_name': product_name or empty_product_label,
                            'paymenttype_id': payment_type_id,
                            'paymenttype_name': payment_type_name,
                            'datetime': wivo_string_datetime,
                            'value': unit_price,
                        }

                        payment_records.append(payment_record)
                        prices_records.append(price_record)
                        costs_records.append(cost_record)
                        costs_units_records.append(cost_unit_record)
                        # only process the first one
                        break

            return [
                {'metric': self._mapping_metric_name('payment'), 'records': payment_records},
                {'metric': self._mapping_metric_name('price'), 'records': prices_records},
                {'metric': self._mapping_metric_name('cost'), 'records': costs_records},
                {'metric': self._mapping_metric_name('paymentunit'), 'records': costs_units_records},
            ]


class BsalePrices(GenericMetricsObjectsCSVStorage, BSaleSource, DailyFetcher):

    name = 'bsale-prices'

    # This Fetcher doesnt need a consolidation process
    PROCESS_BY_RANGE_DAYS_ENABLE = False

    EXPANDED_FIELDS = [
        'variant',
    ]
    LIMIT = 200

    MAPPING_METRIC_NAMES = {}

    @classmethod
    def settings(cls):
        import os
        tokens = os.environ['TOKENS'].split(';')
        storage_path = os.environ['STORAGE_PATH']
        timezone_name = os.environ['TIMEZONE_NAME']
        price_list_id = os.environ['PRICE_LIST_ID']
        return {
            'tokens': tokens,
            'storage_path': storage_path,
            'timezone_name': timezone_name,
            'price_list_id': price_list_id,
        }

    def _mapping_metric_name(self, metric_name):
        if metric_name in self.MAPPING_METRIC_NAMES:
            return self.MAPPING_METRIC_NAMES[metric_name]
        return metric_name

    def parse_data(self, connector, **kwargs):
        price_list_id = kwargs['price_list_id']
        list_prices = []
        count = 0
        for data in connector.get_prices(
                price_list_id,
                expanded_fields=self.EXPANDED_FIELDS,
                limit=self.LIMIT):
            logger.info('Items Total {} offset {}'.format(data['count'], data['offset']))
            for item in data['items']:
                count += 1
                logger.debug('Processing {} Prices ID {}'.format(count, item['id']))

                prices = {
                    'product_id': item['variant']['code'],
                    'datetime': datetime_to_wivo_format(self.actual_datetime_tz),
                    'value': item['variantValueWithTaxes'],
                }
                list_prices.append(prices)

        return [
            {
                'metric': 'price',
                'records': list_prices,
            },
        ]
