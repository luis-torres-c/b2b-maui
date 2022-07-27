import xmltodict
import urllib.parse
import time
import requests

from core.connectors import Connector
from core.utils import makes_request
from utils.logger import logger


class ManagerConnector(Connector):

    URL_BASE = 'https://api.manager.cl'
    GET_DOCUMENTS = '/sec/prod/documentos.asmx/obtenerDocumentos?rutEmpresa={}&token={}&fechaInicial={}&fechaTermino={}&tipoDocumento={}'
    URL_PRODUCTS = 'http://wss.imanager.cl:8081/manager/ws/erp_v2/productos.asmx/ConsultaProd?rutEmpresa={}'
    URL_STOCK = 'http://wss.imanager.cl:8081/manager/ws/erp_v2/productos.asmx/consultaPrecioStock?rutEmpresa={}&pm_codigo={}'
    URL_HEADER = 'http://wss.imanager.cl:8081/manager/ws/erp_v2/documentos.asmx/SeleccionaNumerosDocumentos?rutEmpresa={}&fechaDesde={}&fechaHasta={}&codigoDocumento={}'
    URL_DETAIL = 'http://wss.imanager.cl:8081/manager/ws/erp_v2/documentos.asmx/SeleccionaDetallesDocumento?rutEmpresa={}&tipoDocumento={}&numDocumento={}'

    TIMEOUT = 1200

    @classmethod
    def get_instance(cls, **kwargs):
        rut = kwargs['rut_empresa']
        token = kwargs['token']
        return cls(rut, token)

    def __init__(self, rut, token):
        self.rut = rut
        self.token = token

    def get_documents(self, date, tipoDoc='BOV'):
        params = self.GET_DOCUMENTS.format(
            self.rut, self.token, date, date, tipoDoc)
        resp = makes_request(self.URL_BASE + params, to_json=False, timeout=self.TIMEOUT)
        # if resp['status'] != '200':
        if resp.status_code != requests.codes.ok:
            logger.error('Error {}'.format(resp.status_code))
            return()
        d = xmltodict.parse(resp.content)
        return d

    def get_header_documents(self, date, tipo_doc):
        params = self.URL_HEADER.format(self.rut, date, date, tipo_doc)
        resp = makes_request(self.URL_BASE + params, to_json=False, timeout=self.TIMEOUT)
        if resp.status_code != requests.codes.ok:
            logger.error('Error {}'.format(resp.status_code))
            return()
        d = xmltodict.parse(resp.content)
        return d

    def get_detail_document(self, tipo_doc, num):
        def _retry(_error, _tipo_doc, _num):
            logger.error('Error {}'.format(_error))
            logger.info('Waiting 5 minutes to proceed')
            time.sleep(300)
            return self.get_detail_document(_tipo_doc, _num)

        params = self.URL_DETAIL.format(self.rut, tipo_doc, num)
        try:
            resp = makes_request(params, to_json=False, timeout=self.TIMEOUT)
        except requests.exceptions.ConnectionError as e:
            return _retry(e, tipo_doc, num)
        if resp.status_code != requests.codes.ok:
            return _retry(resp.status_code, tipo_doc, num)
        d = xmltodict.parse(resp.content)
        return d

    def get_stock(self):
        url_products = self.URL_PRODUCTS.format(self.rut)
        logger.info("Obtaining product list from {}".format(url_products))
        resp = makes_request(url_products, to_json=False, timeout=self.TIMEOUT)
        if resp.status_code != requests.codes.ok:
            logger.error('Error {}'.format(resp.status_code))
            return()
        d = xmltodict.parse(resp.content)
        products = list()
        for num, product in enumerate(
                d['ArrayOfConsultaProd']['consultaProd']):
            url_stock_product = self.URL_STOCK.format(
                self.rut, urllib.parse.quote_plus(product['codigo']))
            logger.info("{}/{} Get stock from url {}".format(num, len(d['ArrayOfConsultaProd']['consultaProd']), url_stock_product))
            try:
                resp = makes_request(url_stock_product, to_json=False, timeout=self.TIMEOUT)
            except requests.exceptions.ConnectionError as e:
                # One try only
                logger.error('Error {}'.format(e))
                logger.info('Waiting 5 minutes to proceed')
                time.sleep(300)
                resp = makes_request(url_stock_product, to_json=False, timeout=self.TIMEOUT)

            if resp.status_code != requests.codes.ok:
                logger.error('Error {}'.format(resp.status_code))
                return()
            result = xmltodict.parse(resp.content)
            product['precioUnitario'] = float(
                result['ProductoPrecioStock']['precioUnitario'])
            product['stock'] = result['ProductoPrecioStock']['stockFisico']
            products.append(product)
        return products
