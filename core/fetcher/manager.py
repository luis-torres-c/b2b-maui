import os
import xmltodict
import datetime
import calendar
import time

from core.fetcher import DailyFetcher
from core.storages.onlycsv import OnlyCsvStorage
from core.sources.manager import ManagerSource
from core.utils import timestamp_to_wivo_format, create_id
from utils.logger import logger


class ManagerSales(
        OnlyCsvStorage,
        ManagerSource,
        DailyFetcher):

    name = 'manager-sales'
    HISTORICAL_CACHE_ENABLE = True

    TIPO_DOC = ['BOV', 'FAV', 'NCV']

    @classmethod
    def settings(cls):
        rut_empresa = os.environ['MANAGER_RUT_EMPRESA']
        token = os.environ['MANAGER_TOKEN']
        storage_path = os.environ['STORAGE_PATH']
        timezone_name = os.environ['TIMEZONE_NAME']
        return {
            'rut_empresa': rut_empresa,
            'token': token,
            'storage_path': storage_path,
            'timezone_name': timezone_name
        }

    def parse_data(self, connector, **kwargs):

        string_date = self.actual_date_tz.strftime('%d/%m/%Y')
        sale = []
        salesunit = []
        totalsale = []

        count_zero_values = 0

        for tipo_doc in self.TIPO_DOC:

            content = connector.get_documents(string_date, tipo_doc)

            if content['transaccionResult']['Ok'] == 'true':
                data = xmltodict.parse(content['transaccionResult']['Data'])
                if 'Documento' not in data['Documentos']:
                    logger.info(
                        "No data for document type {} for this day".format(tipo_doc))
                    continue
                if not isinstance(data['Documentos']['Documento'], list):
                    data['Documentos']['Documento'] = [
                        data['Documentos']['Documento'], ]
                for document in data['Documentos']['Documento']:
                    dt = datetime.datetime.strptime(
                        document['cabecera']['fecha'], '%d-%m-%Y %H:%M:%S')
                    date_time = calendar.timegm(dt.utctimetuple())
                    string_datetime = timestamp_to_wivo_format(date_time)
                    bstore_name = document['cabecera']['sucursal'] or 'Sin tienda'
                    rutCliente = document['cabecera']['rutCliente'] if 'rutCliente' in document['cabecera'] else ''
                    razonSocial = document['cabecera']['razonSocial'] if 'razonSocial' in document['cabecera'] else ''

                    base_data = {
                        'datetime': string_datetime,
                        'ticket_id': document['cabecera']['idDocumento'],
                        'bstore_id': create_id(bstore_name),
                        'bstore_name': bstore_name,
                        'seller_id': document['cabecera']['codVend'],
                        # INT-419
                        # 'seller_name': document['cabecera']['codVend'],
                        'client_id': rutCliente,
                        'client_name': rutCliente,
                        'clientname_id': rutCliente,
                        'clientname_name': razonSocial,
                    }
                    if 'detalles' not in document or not document[
                            'detalles'] or 'Detalle' not in document['detalles']:
                        logger.info(
                            "No detail for document type {} ticket {}".format(
                                tipo_doc, document['cabecera']['idDocumento']))
                        continue
                    if not isinstance(document['detalles']['Detalle'], list):
                        document['detalles']['Detalle'] = [
                            document['detalles']['Detalle']]
                    sale_values = dict()
                    total_numeric_discount = 0
                    detail = connector.get_detail_document(
                        tipo_doc, document['cabecera']['numeroDocumento'])
                    if 'ArrayOfDocumentoDetalle' in detail and 'documentoDetalle' in detail[
                            'ArrayOfDocumentoDetalle']:
                        if not isinstance(
                                detail['ArrayOfDocumentoDetalle']['documentoDetalle'], list):
                            detail['ArrayOfDocumentoDetalle']['documentoDetalle'] = [
                                detail['ArrayOfDocumentoDetalle']['documentoDetalle']]
                        for prod_detail in detail['ArrayOfDocumentoDetalle']['documentoDetalle']:
                            sale_values[prod_detail['Código']] = float(prod_detail['Neto']) * (1 - float(prod_detail['DescuentoProducto']) / 100)
                            if prod_detail['TipoDescuento'] == '1':
                                sale_values[prod_detail['Código']] = sale_values[prod_detail['Código']] * (1 - float(prod_detail['Descuento']) / 100)
                            elif prod_detail['TipoDescuento'] == '0':
                                total_numeric_discount = float(prod_detail['Descuento'])
                    for detalle in document['detalles']['Detalle']:
                        sale_row = {
                            'value': sale_values[detalle['codigoProducto']] if detalle['codigoProducto'] in sale_values else 0,
                            'product_id': detalle['codigoProducto'],
                            'product_name': detalle['descripcionProducto'].strip() or 'SIN NOMBRE',
                            'codproduct_id': detalle['codigoProducto'],
                            'codproduct_name': detalle['codigoProducto']
                        }
                        # Manager connector have a strees time, in this time all sales value are 0,
                        # this problem have a time average of 5 minutes
                        if sale_row['value'] == 0.0:
                            count_zero_values += 1
                            if count_zero_values > 10:
                                logger.warning("Values 0 are found in metric sales, waiting 5 minutes to proceed...")
                                time.sleep(300)
                                return self.parse_data(connector, **kwargs)
                            else:
                                count_zero_values = 0
                        # endfix
                        saleunit_row = {
                            'value': float(detalle['cantidad']),
                            'product_id': detalle['codigoProducto'],
                            'product_name': detalle['descripcionProducto'].strip() or 'SIN NOMBRE',
                            'codproduct_id': detalle['codigoProducto'],
                            'codproduct_name': detalle['codigoProducto']
                        }
                        if tipo_doc == 'NCV':
                            sale_row['value'] = - sale_row['value']
                            saleunit_row['value'] = - sale_row['value']
                        sale.append({**base_data, **sale_row})
                        salesunit.append({**base_data, **saleunit_row})
                    totalsale_row = {
                        'value': float(document['cabecera']['totalDocumento']),
                    }
                    if total_numeric_discount != 0:
                        discount_sale = {
                            'value': - total_numeric_discount,
                            'product_id': 'descuentodocumento',
                            'product_name': 'Descuento Documento',
                            'codproduct_id': 'descuentodocumento',
                            'codproduct_name': 'descuentodocumento',
                        }
                        sale.append({**base_data, **discount_sale})
                    if tipo_doc == 'NCV':
                        totalsale_row['value'] = - totalsale_row['value']
                    totalsale.append({**base_data, **totalsale_row})

            else:
                logger.error("Error getting data from source")
                return []

        return [{'metric': 'sale', 'records': sale},
                {'metric': 'salesunit', 'records': salesunit},
                {'metric': 'totalsale', 'records': totalsale}]


class ManagerStock(
        OnlyCsvStorage,
        ManagerSource,
        DailyFetcher):

    name = 'manager-stock'
    HISTORICAL_CACHE_ENABLE = True

    @classmethod
    def settings(cls):
        rut_empresa = os.environ['MANAGER_RUT_EMPRESA']
        token = os.environ['MANAGER_TOKEN']
        storage_path = os.environ['STORAGE_PATH']
        timezone_name = os.environ['TIMEZONE_NAME']
        return {
            'rut_empresa': rut_empresa,
            'storage_path': storage_path,
            'token': token,
            'timezone_name': timezone_name
        }

    def parse_data(self, connector, **kwargs):
        stock = []
        stockunit = []
        content = connector.get_stock()
        for product in content:
            base_data = {
                'datetime': self.actual_date_tz.strftime('%Y-%m-%dT%H:%M:%S'),
                'product_id': product['codigo'],
                'product_name': product['nombre'] or 'SIN NOMBRE',
                'codproduct_id': product['codigo'],
                'codproduct_name': product['codigo'],
            }
            if product['stock'] is None:
                continue
            if not isinstance(product['stock']['RegistroStockFisico'], list):
                product['stock']['RegistroStockFisico'] = [
                    product['stock']['RegistroStockFisico']]
            for storage in product['stock']['RegistroStockFisico']:
                storage_base_data = {
                    'storage_id': create_id(storage['bodega']),
                    'storage_name': storage['bodega'],
                }
                stock_value = {
                    'value': int(storage['stockFisico']) * float(product['precioUnitario'])
                }
                stockunit_value = {
                    'value': int(storage['stockFisico'])
                }

                stock.append({**base_data, **storage_base_data, **stock_value})
                stockunit.append({**base_data, **storage_base_data, **stockunit_value})

        return [{'metric': 'stock', 'records': stock}, {
            'metric': 'stockunit', 'records': stockunit}]
