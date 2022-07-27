import csv
import datetime
import io
import os
import requests
import zipfile

import xml.etree.ElementTree as ET

from core.connectors.b2b.base import B2BConnector
from core.connectors.b2b.base import BBReCommerceFileConnector

from core.connectors.b2b.utils import do_request, ConnectorB2BLoginErrorConnection, SaveErrorLogWhenBadCredentials

from utils.logger import logger


class CoronaB2BConnector(B2BConnector):
    BASE_URL = 'https://proveedores.corona.cl/BBRe-commerce/'
    LOGIN_PATH = 'access/login.do'
    USER_MANAGER_PATH = 'swf/services/UserManagerServer'
    MESSAGING_MANAGER_PATH = 'MessagingManagerServerService/MessagingManagerServer'
    COMMERCIAL_MANAGER_PATH = 'swf/services/CommercialManagerServer'
    DOWNLOAD_PATH = 'download/'

    PORTAL = 'Corona'

    FILE_NAME_PATH = 'b2b-files/{portal}/ventas/{year}/{month}/{filetype}_{client}_{empresa}_{date_from}_{date_to}_{timestamp}.csv'

    @classmethod
    def get_instance(cls, **kwargs):
        b2b_username = kwargs['b2b_username']
        b2b_password = kwargs['b2b_password']
        b2b_empresa = kwargs['b2b_empresa']

        return cls(b2b_username, b2b_password, b2b_empresa)

    def __init__(self,
                 b2b_username,
                 b2b_password,
                 b2b_empresa,
                 ):

        self.b2b_username = b2b_username
        self.b2b_password = b2b_password
        self.b2b_empresa = b2b_empresa

    def login(self, sessions_requests):
        self.check_login_credentials(self.b2b_username, self.b2b_password)

        url_login = self.BASE_URL + self.LOGIN_PATH

        payload_login = {
            "event": "login",
            "companyid": str(),
            "logid": self.b2b_username,
            "password": self.b2b_password,
            "email": str()
        }

        do_request(url_login, sessions_requests, 'GET')
        login = do_request(url_login, sessions_requests, 'POST', payload_login)

        if not login.ok or b'Debe ingresar un RUT v\xe1lido' in login.content:
            SaveErrorLogWhenBadCredentials(
                login.content, os.environ['SOURCE_INT_PATH'], 'Corona')
            args = {
                'username': self.b2b_username,
                'portal': 'Corona',
            }
            raise ConnectorB2BLoginErrorConnection(args)

        return login

    def generate_files(self, **kwargs):
        commercial_manager_url = self.BASE_URL + self.COMMERCIAL_MANAGER_PATH
        sessions_requests = requests.session()

        self.login(sessions_requests)

        header_requests = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-MX,es;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'close',
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': '""'}

        xml_id = """
        <SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"
        xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <SOAP-ENV:Body>
                <tns:findProvidersByUserId xmlns:tns="http://interfaces.webservices.portal.regional.b2b.bbr"/>
            </SOAP-ENV:Body>
        </SOAP-ENV:Envelope>"""

        response = do_request(
            commercial_manager_url,
            sessions_requests,
            'POST',
            payload=xml_id,
            headers=header_requests
        )

        root = ET.fromstring(response.content)
        companycode = root.find(
            '{http://schemas.xmlsoap.org/soap/envelope/}Body/'
            '{http://interfaces.webservices.portal.regional.b2b.bbr}findProvidersByUserIdResponse/'
            '{http://interfaces.webservices.portal.regional.b2b.bbr}out/'
            '{http://classes.data.commercial.regional.b2b.bbr}lastProviderSelected/'
            '{http://classes.data.commercial.regional.b2b.bbr}pvgkey').text

        date_format = '%Y-%m-%d'
        date_from = datetime.datetime.strptime(kwargs['from'], date_format)
        date_to = datetime.datetime.strptime(kwargs['to'], date_format)

        periods_checked = list()

        while date_from <= date_to:

            # This is a tricky behavior. Corona request's payload need month - 1 because in their end works this way.
            # for example
            # if we want results from 2018-05-01, we must send 2018-04-01 in the payload. It's a hardcoded behavior in
            # corona portal
            day = date_from.day
            month = str(int(date_from.month) - 1)
            year = date_from.year

            xml = f"""
            <SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <SOAP-ENV:Body>
                    <tns:downloadSalesProductLocalReport xmlns:tns="http://interfaces.webservices.portal.regional.b2b.bbr">
                        <tns:in0>
                            <ns2:activeProducts xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">true</ns2:activeProducts>
                            <ns2:catLocRetailer xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">true</ns2:catLocRetailer>
                            <ns2:catProdRetailer xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">true</ns2:catProdRetailer>
                            <ns2:excludeProductsWithoutInventory xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">false</ns2:excludeProductsWithoutInventory>
                            <ns2:excludeProductsWithoutSales xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">false</ns2:excludeProductsWithoutSales>
                            <ns2:formatType xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">csv</ns2:formatType>
                            <ns2:groupByLocal xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">true</ns2:groupByLocal>
                            <ns2:groupByProduct xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">true</ns2:groupByProduct>
                            <ns2:highLimitDateSales xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">
                                <ns1:day xmlns:ns1="http://classes.data.commercial.regional.b2b.bbr">{day}</ns1:day>
                                <ns1:month xmlns:ns1="http://classes.data.commercial.regional.b2b.bbr">{month}</ns1:month>
                                <ns1:year xmlns:ns1="http://classes.data.commercial.regional.b2b.bbr">{year}</ns1:year>
                            </ns2:highLimitDateSales>
                            <ns2:keyLocal xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">-1</ns2:keyLocal>
                            <ns2:keyProduct xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">-1</ns2:keyProduct>
                            <ns2:levelLocal xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">0</ns2:levelLocal>
                            <ns2:levelProduct xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">0</ns2:levelProduct>
                            <ns2:levelToGroup xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">-1</ns2:levelToGroup>
                            <ns2:levelToGroupLocal xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">-1</ns2:levelToGroupLocal>
                            <ns2:levelToGroupProduct xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">-1</ns2:levelToGroupProduct>
                            <ns2:localOrCategory xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">false</ns2:localOrCategory>
                            <ns2:lowLimitDateSales xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">
                                <ns1:day xmlns:ns1="http://classes.data.commercial.regional.b2b.bbr">{day}</ns1:day>
                                <ns1:month xmlns:ns1="http://classes.data.commercial.regional.b2b.bbr">{month}</ns1:month>
                                <ns1:year xmlns:ns1="http://classes.data.commercial.regional.b2b.bbr">{year}</ns1:year>
                            </ns2:lowLimitDateSales>
                            <ns2:productOrCategory xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">false</ns2:productOrCategory>
                            <ns2:pvkey xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">{companycode}</ns2:pvkey>
                            <ns2:showInventory xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">true</ns2:showInventory>
                            <ns2:typeOfLocal xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">-1</ns2:typeOfLocal>
                            <ns2:typeOfMark xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">-1</ns2:typeOfMark>
                            <ns2:viewProductOrLocal xmlns:ns2="http://classes.data.sales.commercial.regional.b2b.bbr">true</ns2:viewProductOrLocal>
                        </tns:in0>
                    </tns:downloadSalesProductLocalReport>
                </SOAP-ENV:Body>
            </SOAP-ENV:Envelope>
            """

            out_of_month = datetime.datetime.today().date(
            ) - datetime.timedelta(days=30) > date_from.date()

            if out_of_month:
                xml = xml.replace(
                    'downloadSalesProductLocalReport',
                    'downloadSalesDataSourceSalesInventoryReport')
                xml = xml.replace(
                    '</tns:in0>', '</tns:in0><tns:in1>VT</tns:in1>')

            response = do_request(
                commercial_manager_url,
                sessions_requests,
                'POST',
                payload=xml,
                headers=header_requests
            )

            root = ET.fromstring(response.content)

            if out_of_month:
                download_filename = root.find(
                    '{http://schemas.xmlsoap.org/soap/envelope/}Body/'
                    '{http://interfaces.webservices.portal.regional.b2b.bbr}downloadSalesDataSourceSalesInventoryReportResponse/'
                    '{http://interfaces.webservices.portal.regional.b2b.bbr}out/'
                    '{http://classes.data.commercial.regional.b2b.bbr}realfilename').text
                status_code = root.find(
                    '{http://schemas.xmlsoap.org/soap/envelope/}Body/'
                    '{http://interfaces.webservices.portal.regional.b2b.bbr}downloadSalesDataSourceSalesInventoryReportResponse/'
                    '{http://interfaces.webservices.portal.regional.b2b.bbr}out/'
                    '{http://classes.data.commercial.regional.b2b.bbr}statuscode').text
            else:
                download_filename = root.find(
                    '{http://schemas.xmlsoap.org/soap/envelope/}Body/'
                    '{http://interfaces.webservices.portal.regional.b2b.bbr}downloadSalesProductLocalReportResponse/'
                    '{http://interfaces.webservices.portal.regional.b2b.bbr}out/'
                    '{http://classes.data.commercial.regional.b2b.bbr}realfilename').text
                status_code = root.find(
                    '{http://schemas.xmlsoap.org/soap/envelope/}Body/'
                    '{http://interfaces.webservices.portal.regional.b2b.bbr}downloadSalesProductLocalReportResponse/'
                    '{http://interfaces.webservices.portal.regional.b2b.bbr}out/'
                    '{http://classes.data.commercial.regional.b2b.bbr}statuscode').text

            if status_code == 'C1000':
                logger.debug("No data for this day")
                date_from = date_from + datetime.timedelta(days=1)
                continue

            csv_name = download_filename.replace('zip', 'csv')
            download_url = self.BASE_URL + self.DOWNLOAD_PATH + download_filename

            response = do_request(download_url, sessions_requests, 'GET')

            today_date = datetime.datetime.today()
            file_name = self.FILE_NAME_PATH.format(
                portal=self.PORTAL,
                year=today_date.strftime('%Y'),
                month=today_date.strftime('%m'),
                filetype='ventas',
                client=self.b2b_username,
                empresa=self.b2b_empresa,
                date_from=date_from.strftime("%Y-%m-%d"),
                date_to=date_from.strftime("%Y-%m-%d"),
                timestamp=today_date.timestamp())

            file_full_path = os.path.join(kwargs['source_int_path'], file_name)
            os.makedirs(os.path.dirname(file_full_path), exist_ok=True)
            logger.debug("Saving file {}".format(file_full_path))

            z = zipfile.ZipFile(io.BytesIO(response.content))
            z.extractall(os.path.dirname(file_full_path))
            os.rename(os.path.join(os.path.dirname(
                file_full_path), csv_name), file_full_path)

            periods_checked.append({
                'from': date_from.strftime(date_format),
                'to': date_from.strftime(date_format),
                'status': 'ok'
            })
            date_from = date_from + datetime.timedelta(days=1)

        return periods_checked


class CoronaB2BFileConnector(BBReCommerceFileConnector):

    fixed_sub_folder = 'Corona'

    def parse_metrics(self, row):
        row['VTA_PERIODO(u)'] = float(row['VTA_PERIODO(u)'])
        row['VTA_PUBLICO($)'] = float(row['VTA_PUBLICO($)'])

    def get_file_name(self):
        return self.find_file('ventas')


class CoronaB2BStockFileConnector(BBReCommerceFileConnector):

    fixed_sub_folder = 'Corona'

    def parse_metrics(self, row):
        row['INVENTARIO(u)'] = float(row['INVENTARIO(u)'])
        row['INVENTARIO($)'] = float(row['INVENTARIO($)'])

    def get_file_name(self):
        return self.find_file('ventas', per_timestamp=True)


class CoronaB2BOCConnector(CoronaB2BConnector):
    LOGISTIC_MANAGER_PATH = 'swf/services/LogisticManagerServer'

    @classmethod
    def get_instance(cls, **kwargs):
        b2b_username = kwargs['b2b_username']
        b2b_password = kwargs['b2b_password']

        return cls(b2b_username, b2b_password)

    def __init__(self,
                 b2b_username,
                 b2b_password,
                 ):

        self.b2b_username = b2b_username
        self.b2b_password = b2b_password

    def detalle_venta(self):
        logistic_manager_url = self.BASE_URL + self.LOGISTIC_MANAGER_PATH
        commercial_manager_url = self.BASE_URL + self.COMMERCIAL_MANAGER_PATH
        sessions_requests = requests.session()

        login = self.login(sessions_requests)

        if not login:
            args = {
                'username': self.b2b_username,
                'portal': 'Corona',
            }
            raise ConnectorB2BLoginErrorConnection(args)

        header_requests = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-MX,es;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'close',
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': '""'}

        xml_id = """
        <SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"
        xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <SOAP-ENV:Body>
                <tns:findProvidersByUserId xmlns:tns="http://interfaces.webservices.portal.regional.b2b.bbr"/>
            </SOAP-ENV:Body>
        </SOAP-ENV:Envelope>"""

        response = do_request(
            commercial_manager_url,
            sessions_requests,
            'POST',
            payload=xml_id,
            headers=header_requests
        )

        root = ET.fromstring(response.content)
        # companycode = root.find(
        #    '{http://schemas.xmlsoap.org/soap/envelope/}Body/'
        #    '{http://interfaces.webservices.portal.regional.b2b.bbr}findProvidersByUserIdResponse/'
        #    '{http://interfaces.webservices.portal.regional.b2b.bbr}out/'
        #    '{http://classes.data.commercial.regional.b2b.bbr}lastProviderSelected/'
        #    '{http://classes.data.commercial.regional.b2b.bbr}pvgkey').text

        xml = """
        <SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <SOAP-ENV:Body>
    <tns:getOrdersByVendorLocationAndTypeOfFilter xmlns:tns="http://interfaces.webservices.portal.regional.b2b.bbr">
      <tns:in0>
        <ns2:locationId xmlns:ns2="http://classes.report.logistic.corona.b2b.bbr">-1</ns2:locationId>
        <ns2:ocNumber xmlns:ns2="http://classes.report.logistic.corona.b2b.bbr">-1</ns2:ocNumber>
        <ns2:orderby xmlns:ns2="http://classes.report.logistic.corona.b2b.bbr">
          <ns3:OrderCriteriaW xmlns:ns3="http://classes.adtclasses.common.regional.b2b.bbr">
            <ns3:ascending>true</ns3:ascending>
            <ns3:propertyname>ORDERNUMBER</ns3:propertyname>
          </ns3:OrderCriteriaW>
        </ns2:orderby>
        <ns2:orderstatetypeid xmlns:ns2="http://classes.report.logistic.corona.b2b.bbr">4</ns2:orderstatetypeid>
        <ns2:pageNumber xmlns:ns2="http://classes.report.logistic.corona.b2b.bbr">1</ns2:pageNumber>
        <ns2:rows xmlns:ns2="http://classes.report.logistic.corona.b2b.bbr">100</ns2:rows>
        <ns2:since xsi:nil="true" xmlns:ns2="http://classes.report.logistic.corona.b2b.bbr"/>
        <ns2:typeOfFilter xmlns:ns2="http://classes.report.logistic.corona.b2b.bbr">0</ns2:typeOfFilter>
        <ns2:until xsi:nil="true" xmlns:ns2="http://classes.report.logistic.corona.b2b.bbr"/>
        <ns2:vendorId xmlns:ns2="http://classes.report.logistic.corona.b2b.bbr">20</ns2:vendorId>
      </tns:in0>
      <tns:in1>true</tns:in1>
    </tns:getOrdersByVendorLocationAndTypeOfFilter>
  </SOAP-ENV:Body>
</SOAP-ENV:Envelope>"""

        response = do_request(
            logistic_manager_url,
            sessions_requests,
            'POST',
            payload=xml,
            headers=header_requests
        )

        root = ET.fromstring(response.content)
        orderList = root.findall(
            '{http://schemas.xmlsoap.org/soap/envelope/}Body/'
            '{http://interfaces.webservices.portal.regional.b2b.bbr}getOrdersByVendorLocationAndTypeOfFilterResponse/'
            '{http://interfaces.webservices.portal.regional.b2b.bbr}out/'
            '{http://classes.report.logistic.corona.b2b.bbr}orders/'
            '{http://classes.report.logistic.corona.b2b.bbr}OrderReportDataW/')

        orders_id = list()
        for order in orderList:
            if 'id' in order.tag:
                orders_id.append(order.text)
        if orders_id == []:
            return []

        detail_data = list()
        for order_id in orders_id:
            xml = """
            <SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <SOAP-ENV:Body>
    <tns:getPredeliveryDetailCSVReport xmlns:tns="http://interfaces.webservices.portal.regional.b2b.bbr">
      <tns:in0>{}</tns:in0>
      <tns:in1>20</tns:in1>
      <tns:in2>csv</tns:in2>
    </tns:getPredeliveryDetailCSVReport>
  </SOAP-ENV:Body>
</SOAP-ENV:Envelope>""".format(order_id)
            response = do_request(
                logistic_manager_url,
                sessions_requests,
                'POST',
                payload=xml,
                headers=header_requests
            )

            root = ET.fromstring(response.content)
            download_filename = root.find(
                '{http://schemas.xmlsoap.org/soap/envelope/}Body/'
                '{http://interfaces.webservices.portal.regional.b2b.bbr}getPredeliveryDetailCSVReportResponse/'
                '{http://interfaces.webservices.portal.regional.b2b.bbr}out/'
                '{http://classes.report.logistic.corona.b2b.bbr}realfilename').text

            csv_name = download_filename
            download_url = self.BASE_URL + self.DOWNLOAD_PATH + download_filename

            response = do_request(
                download_url,
                sessions_requests,
                'GET',
            )

            csv_file = open(csv_name, 'wb')
            csv_file.write(response.content)
            csv_file.close()

            got_fieldnames = False
            fieldnames = []
            with open(csv_name, 'r', encoding='ISO-8859-15') as f:
                reader = csv.reader(f)
                for row in reader:
                    if not got_fieldnames:
                        for ele in row:
                            fieldnames.append(ele)

                        got_fieldnames = True
                    else:
                        new_row = dict()
                        for ele, field in zip(row, fieldnames):
                            new_row[field] = ele
                        detail_data.append(new_row)

            os.remove(csv_name)

        data = list()
        for order_id in orders_id:
            xml = """<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <SOAP-ENV:Body>
    <tns:getBarcodeReportByOrder xmlns:tns="http://interfaces.webservices.portal.regional.b2b.bbr">
      <tns:in0>{}</tns:in0>
      <tns:in1>20</tns:in1>
      <tns:in2>csv</tns:in2>
    </tns:getBarcodeReportByOrder>
  </SOAP-ENV:Body>
</SOAP-ENV:Envelope>""".format(order_id)
            response = do_request(
                logistic_manager_url,
                sessions_requests,
                'POST',
                payload=xml,
                headers=header_requests
            )

            root = ET.fromstring(response.content)
            download_filename = root.find(
                '{http://schemas.xmlsoap.org/soap/envelope/}Body/'
                '{http://interfaces.webservices.portal.regional.b2b.bbr}getBarcodeReportByOrderResponse/'
                '{http://interfaces.webservices.portal.regional.b2b.bbr}out/'
                '{http://classes.report.logistic.corona.b2b.bbr}realfilename').text

            csv_name = download_filename
            download_url = self.BASE_URL + self.DOWNLOAD_PATH + download_filename

            response = do_request(
                download_url,
                sessions_requests,
                'GET',
            )

            csv_file = open(csv_name, 'wb')
            csv_file.write(response.content)
            csv_file.close()

            got_fieldnames = False
            fieldnames = []
            with open(csv_name, 'r', encoding='ISO-8859-15') as f:
                reader = csv.reader(f)
                for row in reader:
                    if not got_fieldnames:
                        for ele in row:
                            fieldnames.append(ele)

                        got_fieldnames = True
                    else:
                        new_row = dict()
                        for ele, field in zip(row, fieldnames):
                            new_row[field] = ele
                        data.append(new_row)

            os.remove(csv_name)

        for row in detail_data:
            for field in fieldnames:
                row[field] = '' if field not in row else row[field]
        for detail in detail_data:
            for dat in data:
                if detail['Núm. Orden'] == dat['Núm. Orden'] and detail['Cód. Estilo'] == dat['Cod. Corona']:
                    for key in dat.keys():
                        detail[key] = dat[key]

        return detail_data
