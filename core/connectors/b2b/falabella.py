import requests
import re
import pathlib
import datetime
import os
import glob
import csv
from babel.numbers import parse_decimal
from collections import Counter
from itertools import repeat, chain
from bs4 import BeautifulSoup
from core.connectors.b2b.base import B2BConnector
from core.connectors.b2b.utils import do_request
from core.connectors.b2b.utils import ConnectorB2BLoginErrorConnection, SaveErrorLogWhenBadCredentials
from utils.logger import logger


class FalabellaB2BOCConnector(B2BConnector):
    BASE_URL = 'https://b2b.falabella.com/b2bprd/'
    HOME_PATH = 'grafica/html/main_home.html'
    LOGIN_PATH = 'logica/jsp/B2BBF000.jsp?MODULO=4'
    INFORMES_PATH = 'logica/jsp/B2BvFDescarga.do?tipo=eVTA'
    SIGUIENTE_INFORMES = 'logica/jsp/B2BvFDescarga.do?d-16544-p={}&tipo=eVTA'
    INFORME_PRODUCTOS = 'logica/jsp/B2BvFDescarga.do?tipo=eCAT'
    LOGOUT_PATH = 'logica/jsp/B2BvFCerrarSesion.jsp'
    OC_PATH = 'logica/jsp/B2BvFDescarga.do?tipo=eOC'
    SIGUIENTE_OC = 'logica/jsp/B2BvFDescarga.do?d-16544-p={}&tipo=eOC'
    OD_PATH = 'logica/jsp/B2BvFDescarga.do?tipo=eOD'
    SIGUIENTE_OD = 'logica/jsp/B2BvFDescarga.do?d-16544-p={}&tipo=eOD'
    FILE_TIME_OF_LIFE = 82000

    WEEK_DAYS_FOR_WEEKLY_CASE = [
        'LUNES',
        'MARTES',
        'MIERCOLES',
        'JUEVES',
        'VIERNES',
        'SABADO',
        'DOMINGO'
    ]

    WEEK_DAYS_FOR_DAILY_CASE = [
        'LINES',
        'MARTES',
        'MIERCOLE',
        'JUEVES',
        'VIERNES',
        'SABADO',
        'DOMINGO'
    ]

    CADENA = "1"
    DATE_STYLE = "width:'9%';text-align:'center'"

    def login(self, sessions_requests):
        self.check_login_credentials(self.b2b_username, self.b2b_password)

        url_home = self.BASE_URL + self.HOME_PATH
        url_login = self.BASE_URL + self.LOGIN_PATH

        payload_login = {
            "CADENA": self.CADENA,
            "EMPRESA": self.b2b_empresa,
            "USUARIO": self.b2b_username,
            "PASSWORD": self.b2b_password,
            "entrar2": "Entrar",
            "paisSeller": "0"
        }

        do_request(url_home, sessions_requests, 'GET')
        login = do_request(
            url_login,
            sessions_requests,
            'POST',
            payload_login,
            url_home)

        if login.status_code == 302 or 'ERROR' in login.text:
            SaveErrorLogWhenBadCredentials(login.content, os.environ['SOURCE_INT_PATH'], "Falabella")
            arg = {
                'username': self.b2b_username,
                'portal': 'Falabella'
            }
            raise ConnectorB2BLoginErrorConnection(arg)

        return login

    def logout(self, sessions_requests):
        url_logout = self.BASE_URL + self.LOGOUT_PATH
        do_request(url_logout, sessions_requests, 'GET')

    @classmethod
    def get_instance(cls, **kwargs):
        b2b_username = kwargs['b2b_username']
        b2b_empresa = kwargs['b2b_empresa']
        b2b_password = kwargs['b2b_password']
        date_start = kwargs['date_start']
        return cls(
            b2b_username,
            b2b_empresa,
            b2b_password,
            date_start,
        )

    def __init__(self,
                 b2b_username,
                 b2b_empresa,
                 b2b_password,
                 date_start,  # Ej: "13/11/2017"
                 ):

        self.b2b_username = b2b_username
        self.b2b_empresa = b2b_empresa
        self.b2b_password = b2b_password
        self.date_start = date_start
        self.fieldnames = []

    def order_filter(self, href_list):
        result = list()
        id_founds = list()
        for href in href_list:
            id_found = href.split('-')[-1].replace('.txt', '')
            if id_found not in id_founds:
                id_founds.append(id_found)
                result.append(href)
        return result

    def detalle_venta_manual(self):

        data, od_data = self.extract_data_from_files()
        od_data = self.preprocessed_data(od_data=od_data, data=data)

        # data, od_data = self.extract_data()
        # od_data = self.preprocessed_data(od_data=od_data, data=data)
        self.delete_files()
        return od_data

    def detalle_venta(self):

        # data, od_data = self.extract_data_from_files()
        # od_data = self.preprocessed_data(od_data=od_data, data=data)

        data, od_data = self.extract_data()
        od_data = self.preprocessed_data(od_data=od_data, data=data)

        return od_data

    def preprocessed_data(self, od_data, data):
        for row in od_data:
            for field in self.fieldnames:
                row[field] = '' if field not in row else row[field]

        for row in od_data:
            for row2 in data:
                if row2['NRO_OC'] == row['NRO_OC'] and row2['SKU'] == row['SKU']:
                    for key in row2.keys():
                        row[key] = row2[key]
                    continue
        return od_data

    def extract_data_from_files(self):
        download_folder = os.path.join(os.environ['SOURCE_INT_PATH'], f'b2b-files/Falabella/raw/manual-files')
        data = []
        od_data = []
        for file in glob.glob(os.path.join(download_folder, '*eoc*.txt')):
            print("file ", file)
            with open(file, 'r', encoding='ISO-8859-1') as f:
                txt = f.read()
                data += self.get_data_from_txt(txt.strip())

        for file in glob.glob(os.path.join(download_folder, '*eod*.txt')):
            print("file ", file)
            with open(file, 'r', encoding='ISO-8859-1') as f:
                txt = f.read()
                data += self.get_data_from_txt(txt.strip())
        return data, od_data

    def delete_files(self):
        download_folder = os.path.join(os.environ['SOURCE_INT_PATH'], f'b2b-files/Falabella/raw/manual-files')
        for file in glob.glob(os.path.join(download_folder, '*.txt')):
            fcsv = pathlib.Path(file)
            if datetime.datetime.now().today().timestamp() - fcsv.stat().st_mtime > self.FILE_TIME_OF_LIFE:
                os.remove(file)

    def extract_data(self):
        url_informe = self.BASE_URL + self.OC_PATH
        sessions_requests = requests.session()
        login = self.login(sessions_requests)

        if not login:
            args = {
                'username': self.b2b_username,
                'portal': 'Falabella', }
            raise ConnectorB2BLoginErrorConnection(args)

        response = do_request(url_informe, sessions_requests, 'GET')
        soup = BeautifulSoup(response.text, 'html.parser')

        items_text = soup.find(
            'span', attrs={
                'class': 'displayTagTablapagebanner'})

        try:
            if 'Un item' in items_text.text:
                items_amount = 1
            else:
                items_amount = int(re.search(r'\d+', items_text.text).group())
        except AttributeError:
            logger.debug("No data to extract")
            return []

        page = 1

        oc_paths = list()
        while items_amount > 0:
            table = soup.find('table', attrs={'class': 'tablaDatos'})
            table_body = table.find('tbody')
            rows = table_body.find_all('tr')
            for row in rows:
                oc_paths.append(
                    row.find(
                        'td', attrs={
                            'style': "width:'9%';text-align:'center'"}).find(
                        'a', href=True)['href'].replace(
                        '../', ''))
            items_amount -= 15
            page += 1
            next_url = self.BASE_URL + self.SIGUIENTE_OC.format(page)
            response = do_request(next_url, sessions_requests, 'GET')
            soup = BeautifulSoup(response.text, 'html.parser')

        got_fieldnames = False
        data = []
        oc_paths = self.order_filter(oc_paths)
        for oc_path in oc_paths:
            new_document = True
            download_url = self.BASE_URL + oc_path
            raw_data = do_request(download_url, sessions_requests, 'GET')
            raw_data_text = raw_data.text.strip()
            for row in raw_data_text.split('\n'):
                cols = row.split('|')
                if not got_fieldnames:
                    for ele in cols:
                        ele = ele.rstrip()
                        self.fieldnames.append(ele)
                    got_fieldnames = True
                    new_document = False
                elif new_document:
                    # Skip first line
                    new_document = False
                else:
                    new_row = dict()
                    for ele, field in zip(cols, self.fieldnames):
                        ele = ele.rstrip()
                        new_row[field] = ele
                    data.append(new_row)

        # get od data
        url_informe = self.BASE_URL + self.OD_PATH

        response = do_request(url_informe, sessions_requests, 'GET')
        soup = BeautifulSoup(response.text, 'html.parser')

        items_text = soup.find(
            'span', attrs={
                'class': 'displayTagTablapagebanner'})

        if 'Un item' in items_text.text:
            items_amount = 1
        else:
            items_amount = int(re.search(r'\d+', items_text.text).group())

        page = 1

        od_paths = list()
        while items_amount > 0:
            table = soup.find('table', attrs={'class': 'tablaDatos'})
            table_body = table.find('tbody')
            rows = table_body.find_all('tr')
            for row in rows:
                od_paths.append(
                    row.find(
                        'td', attrs={
                            'style': "width:'9%';text-align:'center'"}).find(
                        'a', href=True)['href'].replace(
                        '../', ''))
            items_amount -= 15
            page += 1
            next_url = self.BASE_URL + self.SIGUIENTE_OD.format(page)
            response = do_request(next_url, sessions_requests, 'GET')
            soup = BeautifulSoup(response.text, 'html.parser')

        od_data = []
        got_fieldnames = False
        fieldnames = []
        od_paths = self.order_filter(od_paths)
        for od_path in od_paths:
            new_document = True
            download_url = self.BASE_URL + od_path
            raw_data = do_request(download_url, sessions_requests, 'GET')
            raw_data_text = raw_data.text.strip()
            for row in raw_data_text.split('\n'):
                cols = row.split('|')
                if not got_fieldnames:
                    for ele in cols:
                        ele = ele.rstrip()
                        fieldnames.append(ele)
                    got_fieldnames = True
                    new_document = False
                elif new_document:
                    # Skip first line
                    new_document = False
                else:
                    new_row = dict()
                    for ele, field in zip(cols, fieldnames):
                        ele = ele.rstrip()
                        new_row[field] = ele
                    od_data.append(new_row)
        return data, od_data

    def get_data_from_txt(self, data_txt):
        got_fieldnames = False
        data = []
        for row in data_txt.split('\n'):
            cols = row.split('|')
            if not got_fieldnames:
                for ele in cols:
                    ele = ele.rstrip()
                    self.fieldnames.append(ele)
                got_fieldnames = True
                new_document = False
            elif new_document:
                # Skip first line
                new_document = False
            else:
                new_row = dict()
                for ele, field in zip(cols, self.fieldnames):
                    ele = ele.rstrip()
                    new_row[field] = ele
                data.append(new_row)
        return data

class FalabellaB2BPortalConnector(B2BConnector):
    BASE_URL = 'https://b2b.falabella.com/b2bprd/'
    HOME_PATH = 'grafica/html/main_home.html'
    LOGIN_PATH = 'logica/jsp/B2BBF000.jsp?MODULO=4'
    INFORMES_PATH = 'logica/jsp/B2BvFDescarga.do?tipo=eVTA'
    SIGUIENTE_INFORMES = 'logica/jsp/B2BvFDescarga.do?d-16544-p={}&tipo=eVTA'
    INFORME_PRODUCTOS = 'logica/jsp/B2BvFDescarga.do?tipo=eCAT'
    LOGOUT_PATH = 'logica/jsp/B2BvFCerrarSesion.jsp'

    HISTORICAL_DATA_REQUEST = 'logica/jsp/B2BvFConsultaVentaHistorico.do'
    HISTORICAL_GET_PARAMS = '?d-16544-p={}&accion=inicio'

    CADENA = "1"
    DATE_STYLE = "width:'9%';text-align:'center'"
    PORTAL = 'Falabella'

    FILE_NAME_PATH = 'b2b-files/{portal}/ventas/{year}/{month}/{filetype}_{client}_{empresa}_{date_from}_{date_to}_{timestamp}.csv'

    FILE_NAME_SEARCH_PATTERN = 'b2b-files/{portal}/ventas/**/{filetype}_{client}_{empresa}_*.csv'

    WEEK_DAYS_FOR_WEEKLY_CASE = [
        'LUNES',
        'MARTES',
        'MIERCOLES',
        'JUEVES',
        'VIERNES',
        'SABADO',
        'DOMINGO'
    ]

    WEEK_DAYS_FOR_DAILY_CASE = [
        'LINES',
        'MARTES',
        'MIERCOLE',
        'JUEVES',
        'VIERNES',
        'SABADO',
        'DOMINGO'
    ]

    @classmethod
    def get_instance(cls, **kwargs):
        b2b_username = kwargs['b2b_username']
        b2b_empresa = kwargs['b2b_empresa']
        b2b_password = kwargs['b2b_password']
        return cls(
            b2b_username,
            b2b_empresa,
            b2b_password)

    def __init__(self,
                 b2b_username,
                 b2b_empresa,
                 b2b_password):

        self.b2b_username = b2b_username
        self.b2b_empresa = b2b_empresa
        self.b2b_password = b2b_password

    def login(self, sessions_requests):
        self.check_login_credentials(self.b2b_username, self.b2b_password)

        url_home = self.BASE_URL + self.HOME_PATH
        url_login = self.BASE_URL + self.LOGIN_PATH

        payload_login = {
            "CADENA": self.CADENA,
            "EMPRESA": self.b2b_empresa,
            "USUARIO": self.b2b_username,
            "PASSWORD": self.b2b_password,
            "entrar2": "Entrar",
            "paisSeller": "0"
        }

        do_request(url_home, sessions_requests, 'GET')
        login = do_request(
            url_login,
            sessions_requests,
            'POST',
            payload_login,
            url_home)

        if login.status_code == 302 or 'ERROR' in login.text:
            SaveErrorLogWhenBadCredentials(login.content, os.environ['SOURCE_INT_PATH'], "Falabella")
            arg = {
                'username': self.b2b_username,
                'portal': 'Falabella'
            }
            raise ConnectorB2BLoginErrorConnection(arg)

        return login

    def logout(self, sessions_requests):
        url_logout = self.BASE_URL + self.LOGOUT_PATH
        do_request(url_logout, sessions_requests, 'GET')

    def create_actual_sale_files(self, sessions_requests, **kwargs):
        periods_checked = list()

        url_informe = self.BASE_URL + self.INFORMES_PATH
        response = do_request(url_informe, sessions_requests, 'GET')
        soup = BeautifulSoup(response.text, 'html.parser')
        items_text = soup.find('span', attrs={'class': 'displayTagTablapagebanner'})

        if not items_text:
            logger.debug("No Actual Files Availables to Download")
            return periods_checked

        items_amount = int(re.search(r'\d+', items_text.text).group())

        self.by_week = items_amount <= 6

        page = 1
        url_files = list()
        while items_amount > 0:
            for tag_a in soup.findAll('a', href=True):
                if '.txt' in tag_a['href']:
                    url_files.append(tag_a['href'].replace('../', ''))
            items_amount -= 15
            page += 1
            next_url = self.BASE_URL + self.SIGUIENTE_INFORMES.format(page)
            response = do_request(next_url, sessions_requests, 'GET')
            soup = BeautifulSoup(response.text, 'html.parser')

        today_date = datetime.datetime.today()
        for url_sales_file in url_files:

            download_url = self.BASE_URL + url_sales_file
            file_temp_path = os.path.join(kwargs['source_int_path'], 'temp.csv')
            if os.path.exists(file_temp_path):
                os.remove(file_temp_path)

            os.makedirs(os.path.dirname(file_temp_path), exist_ok=True)
            with open(file_temp_path, 'w') as csvfile:
                raw_data = do_request(download_url, sessions_requests, 'GET')
                logger.debug("Saving temp file {}".format(file_temp_path))
                csvfile.write(raw_data.text.strip())

            filename_date_text = url_sales_file.split('-')[-1].replace('.txt', '')
            filename_date_datetime = datetime.datetime.strptime(filename_date_text, '%Y%m%d')

            with open(file_temp_path, "r") as csvfile:
                reader = csv.DictReader(csvfile, delimiter='|')
                headers = reader.fieldnames

            days_in_file = list()
            for h in headers:
                if self.by_week:
                    for day_of_week in self.WEEK_DAYS_FOR_WEEKLY_CASE:
                        if day_of_week in h:
                            days_in_file.append(h)
                else:
                    for day_of_week in self.WEEK_DAYS_FOR_DAILY_CASE:
                        if day_of_week in h:
                            days_in_file.append(h)

            date_format_filename = '-' if self.by_week else '_'

            date_t = filename_date_datetime - datetime.timedelta(days=1)
            this_day, this_month = [int(k) for k in days_in_file[-1].split('_', 1)[1].split(date_format_filename)]
            date_t.replace(month=this_month, day=this_day)
            date_t = date_t.strftime('%Y-%m-%d')
            if len(days_in_file) == 1:
                date_f = date_t

            elif len(days_in_file) > 1:
                date_f = (datetime.datetime.strptime(date_t, '%Y-%m-%d') - datetime.timedelta(days=6)).strftime('%Y-%m-%d')

            else:
                logger.error("Cannot calculate days of the data")

            file_name = self.FILE_NAME_PATH.format(
                portal=self.PORTAL,
                year=today_date.strftime('%Y'),
                month=today_date.strftime('%m'),
                filetype='ventas',
                client=kwargs['b2b_username'],
                empresa=kwargs['b2b_empresa'],
                date_from=date_f,
                date_to=date_t,
                timestamp=today_date.timestamp())
            file_full_path = os.path.join(kwargs['source_int_path'], file_name)
            os.makedirs(os.path.dirname(file_full_path), exist_ok=True)

            logger.debug("Renaming temp file to {}".format(file_full_path))
            os.rename(file_temp_path, file_full_path)
            if self.check_if_csv(file_full_path, delimiter='|'):
                periods_checked.append({
                    'from': date_f,
                    'to': date_t,
                    'status': 'ok',
                })
            else:
                logger.debug("It was an error creating file {}, trying again..".format(file_full_path))
                os.remove(file_full_path)

        return periods_checked

    def create_product_file(self, sessions_requests, **kwargs):
        url_informe_products = self.BASE_URL + self.INFORME_PRODUCTOS
        today_date = datetime.datetime.today()

        response = do_request(url_informe_products, sessions_requests, 'GET')
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', attrs={'class': 'tablaDatos'})
        table_body = table.find('tbody')
        row = table_body.find_all('tr')[0]
        date = row.find('td', attrs={'style': self.DATE_STYLE})
        href = date.find('a', href=True)
        download_path = href['href']
        download_path = download_path.replace('../', '')
        download_url = self.BASE_URL + download_path
        file_name = self.FILE_NAME_PATH.format(
            portal=self.PORTAL,
            year=today_date.strftime('%Y'),
            month=today_date.strftime('%m'),
            filetype='products',
            client=kwargs['b2b_username'],
            empresa=kwargs['b2b_empresa'],
            date_from=kwargs['from'],
            date_to=kwargs['to'],
            timestamp=today_date.timestamp()
        )
        file_full_path = os.path.join(kwargs['source_int_path'], file_name)
        os.makedirs(os.path.dirname(file_full_path), exist_ok=True)
        with open(file_full_path, 'w') as csvfile:
            raw_data = do_request(download_url, sessions_requests, 'GET')
            logger.debug("Saving file {}".format(file_full_path))
            csvfile.write(raw_data.text.strip())

    def create_historical_sale_files(self, sessions_requests, page, **kwargs):
        periods_checked = list()
        today_date = datetime.datetime.today()

        url_historical_files = self.BASE_URL + self.HISTORICAL_DATA_REQUEST + self.HISTORICAL_GET_PARAMS
        url_files_to_download = list()
        response = do_request(url_historical_files.format(page), sessions_requests, 'GET')
        soup = BeautifulSoup(response.text, 'html.parser')
        tablas = soup.findAll(class_='tablaDatos')
        for tabla in tablas:
            for a in tabla.findAll('a', href=True):
                url_files_to_download.append(a['href'].replace('../', ''))
        for url_file in url_files_to_download:
            filename_date_text = url_file.split('-')[-1].replace('.txt', '')
            filename_date_datetime = datetime.datetime.strptime(filename_date_text, '%Y%m%d')
            date_t = filename_date_datetime.strftime('%Y-%m-%d')
            date_f = (filename_date_datetime - datetime.timedelta(days=6)).strftime('%Y-%m-%d')
            download_url = self.BASE_URL + url_file
            file_name = self.FILE_NAME_PATH.format(
                portal=self.PORTAL,
                year=today_date.strftime('%Y'),
                month=today_date.strftime('%m'),
                filetype='ventas',
                client=kwargs['b2b_username'],
                empresa=kwargs['b2b_empresa'],
                date_from=date_f,
                date_to=date_t,
                timestamp=today_date.timestamp())
            file_full_path = os.path.join(kwargs['source_int_path'], file_name)
            os.makedirs(os.path.dirname(file_full_path), exist_ok=True)
            with open(file_full_path, 'w') as csvfile:
                raw_data = do_request(download_url, sessions_requests, 'GET')
                logger.debug("Saving file {}".format(file_full_path))
                csvfile.write(raw_data.text.strip())
            periods_checked.append({
                'from': date_f,
                'to': date_t,
                'status': 'ok',
            })

        if soup.find('a', text='Próximo', href=True) and page != 15:
            return periods_checked + self.create_historical_sale_files(sessions_requests, page + 1, **kwargs)

        return periods_checked

    def check_periods_requested(self, sessions_requests, page, **kwargs):
        period_requested = []
        url_historical_files = self.BASE_URL + self.HISTORICAL_DATA_REQUEST + self.HISTORICAL_GET_PARAMS
        response = do_request(url_historical_files.format(page), sessions_requests, 'GET')
        soup = BeautifulSoup(response.text, 'html.parser')
        tablas = soup.findAll(class_='tablaDatos')
        for tabla in tablas:
            if tabla.find('th', class_='order1'):
                for row in tabla.findAll('td'):
                    if '/' in row.text:
                        period_requested.append(row.text)
        if soup.find('a', text='Próximo', href=True) and page != 15:
            return period_requested + self.check_periods_requested(sessions_requests, page + 1, **kwargs)
        return period_requested

    def request_month(self, sessions_requests, date):
        logger.debug('Request historical data to date {}'.format(date))
        url_request_historical_data = self.BASE_URL + self.HISTORICAL_DATA_REQUEST
        params = {
            'accion': 'consulta',
            'disponibles': 2,
            'anno': date.split('-')[0],
            'mes': date.split('-')[1]
        }
        response = do_request(url_request_historical_data, sessions_requests, 'POST', params)
        soup = BeautifulSoup(response.text, 'html.parser')
        message = soup.find('td', class_='texto-alert')
        if message:
            logger.debug(message.text)

    def generate_files(self, **kwargs):
        today_date = datetime.datetime.today()
        periods_checked = list()
        self.by_week = False

        sessions_requests = requests.session()

        self.login(sessions_requests)

        periods_checked = self.create_actual_sale_files(sessions_requests, **kwargs)

        self.create_product_file(sessions_requests, **kwargs)

        periods_checked += self.create_historical_sale_files(sessions_requests, 1, **kwargs)

        # Check if period request is completed with the files downloaded
        request_from = datetime.datetime.strptime(kwargs['from'], '%Y-%m-%d')
        request_to = datetime.datetime.strptime(kwargs['to'], '%Y-%m-%d')
        request_list = list()
        while request_from <= request_to:
            request_list.append(request_from)
            request_from += datetime.timedelta(days=1)

        if self.by_week:
            periods_checked.append({
                'from': (today_date + datetime.timedelta(days=-today_date.weekday())).strftime('%Y-%m-%d'),
                'to': (today_date + datetime.timedelta(days=-1)).strftime('%Y-%m-%d'),
                'status': 'ok'})

        for check in periods_checked:
            check_from = datetime.datetime.strptime(check['from'], '%Y-%m-%d')
            check_to = datetime.datetime.strptime(check['to'], '%Y-%m-%d')
            while check_from <= check_to:
                if check_from in request_list:
                    request_list.remove(check_from)
                check_from += datetime.timedelta(days=1)

            if not request_list:
                return periods_checked

        # if False, then Check if some old file completed the period

        file_path = os.path.join(kwargs['source_int_path'], self.FILE_NAME_SEARCH_PATTERN.format(portal=self.PORTAL, filetype='ventas', client=self.b2b_username, empresa=self.b2b_empresa))
        files = glob.glob(file_path, recursive=True)
        for filename in files:
            date_from = datetime.datetime.strptime(filename.split('_')[3], '%Y-%m-%d')
            date_to = datetime.datetime.strptime(filename.split('_')[4], '%Y-%m-%d')
            while date_from <= date_to:
                if date_from in request_list:
                    request_list.remove(date_from)
                date_from += datetime.timedelta(days=1)

            if not request_list:
                return periods_checked

        # if False, then Check if can request months in portal (consider if the month already are requested)
        periods_requested = self.check_periods_requested(sessions_requests, 1, **kwargs)
        periods_requested_list = list(dict.fromkeys(periods_requested))

        for requested in periods_requested_list:
            logged = False
            file_period_from = datetime.datetime.strptime(requested, '%d/%m/%Y') - datetime.timedelta(days=6)
            file_period_to = datetime.datetime.strptime(requested, '%d/%m/%Y')
            while file_period_from <= file_period_to:
                if file_period_from in request_list:
                    request_list.remove(file_period_from)
                    if not logged:
                        logger.debug("The period from {} to {} has been requested today".format(file_period_from, file_period_to))
                        logged = True
                file_period_from += datetime.timedelta(days=1)
            if not request_list:
                return periods_checked

        # if True then request month/s, warning if month can't requested
        period_to_request = []
        for request in request_list:
            period_to_request.append(request.strftime('%Y-%m'))
        result = list(chain.from_iterable(repeat(i, c) for i, c in Counter(period_to_request).most_common()))
        result = list(dict.fromkeys(result))
        while len(periods_requested_list) < 8 and 1 <= len(result):
            self.request_month(sessions_requests, result[0])
            del(result[0])
            periods_requested = self.check_periods_requested(sessions_requests, 1, **kwargs)
            periods_requested_list = list(dict.fromkeys(periods_requested))

        self.logout(sessions_requests)

        return periods_checked


class FalabellaB2BFileBaseConnector(B2BConnector):

    file_pattern = '{filetype}_{b2b_username}_{b2b_empresa}_*.csv'
    file_pattern_with_date = '{filetype}_{b2b_username}_{b2b_empresa}_{fecha}_{fecha}_*.csv'
    file_pattern_final_date = '{filetype}_{b2b_username}_{b2b_empresa}_*_{fecha}_*.csv'
    fixed_sub_folder = 'Falabella'
    fixed_sub_root_folder = 'b2b-files'
    fixed_sub_sales_folder = 'ventas'

    @classmethod
    def get_instance(cls, **kwargs):
        query_date = kwargs['date_start']
        repository_path = kwargs['repository_path']
        b2b_username = kwargs['b2b_username']
        b2b_empresa = kwargs['b2b_empresa']
        return cls(query_date, b2b_username, b2b_empresa, repository_path)

    def __init__(self, query_date, b2b_username, b2b_empresa, base_path):
        # FIXME change this horrible thing
        self.date_start = query_date.strftime("%Y-%m-%d 00:00:00 00:00")
        self._date = query_date
        self.user_name = b2b_username
        self.b2b_empresa = b2b_empresa
        self.repository_path = os.path.join(
            base_path, self.fixed_sub_root_folder, self.fixed_sub_folder, self.fixed_sub_sales_folder)

    def find_file(self, filetype, fecha=False, final_date=False, last_generated=False):
        if final_date:
            file_name = self.file_pattern_final_date.format(filetype=filetype, b2b_username=self.user_name, b2b_empresa=self.b2b_empresa, fecha=fecha)
        elif fecha:
            file_name = self.file_pattern_with_date.format(filetype=filetype, b2b_username=self.user_name, b2b_empresa=self.b2b_empresa, fecha=fecha)
        else:
            file_name = self.file_pattern.format(filetype=filetype, b2b_username=self.user_name, b2b_empresa=self.b2b_empresa)
        file_path = os.path.join(self.repository_path, '**', file_name)
        files = glob.glob(file_path, recursive=True)
        file_name = None
        ts_file = 0
        daily_file = None
        for filename in files:
            timestamp_for_file = float(filename.split('_')[-1].split('.csv')[0])
            date_from = datetime.datetime.strptime(filename.split('_')[3], '%Y-%m-%d').date()
            date_to = datetime.datetime.strptime(filename.split('_')[4], '%Y-%m-%d').date()
            if fecha or last_generated or (date_from <= self._date and self._date <= date_to):
                logger.debug("File whit data {}".format(filename))
                if ts_file < timestamp_for_file:
                    file_name = filename
                    ts_file = timestamp_for_file
                    daily_file = (date_from == date_to)
        if file_name:
            logger.debug("File to process {}".format(file_name))
            return (file_name, ts_file, daily_file)
        else:
            logger.debug("No files to process!")
            if filetype == 'products':
                return self.find_file(filetype, last_generated=True)
            return None

    def get_product_values(self):
        result = dict()
        filesearch_result = self.find_file('products')
        if filesearch_result:
            with open(filesearch_result[0], 'r') as csvfile:
                reader = csv.DictReader(csvfile, delimiter='|')
                for row in reader:
                    result[row['SKU'].strip()] = row
        return result


class FalabellaB2BFileConnector(FalabellaB2BFileBaseConnector):

    WEEK_DAYS_FOR_WEEKLY_CASE = [
        'LUNES',
        'MARTES',
        'MIERCOLES',
        'JUEVES',
        'VIERNES',
        'SABADO',
        'DOMINGO'
    ]

    WEEK_DAYS_FOR_DAILY_CASE = [
        'LINES',
        'MARTES',
        'MIERCOLE',
        'JUEVES',
        'VIERNES',
        'SABADO',
        'DOMINGO'
    ]

    # in plural
    MONEDA = 'PESOS'

    def detalle_venta(self):
        filesearch_result = self.find_file('ventas')
        if not filesearch_result:
            return []

        file_name, ts_file, daily_file = filesearch_result

        product_dict = self.get_product_values()

        data = list()

        if daily_file:
            weekday = self._date.weekday()
            days_to_consider = list()
            for i in range(0, weekday + 1):
                days_to_consider.append(self._date - datetime.timedelta(days=i))
            days_to_consider = days_to_consider[::-1]

            # get files from days
            dict_to_search = dict()
            for day in days_to_consider:
                result = self.find_file('ventas', fecha=day.strftime('%Y-%m-%d'))
                if result:
                    dict_to_search[day] = result[0]

            # Get sales and costs from acumulated week
            sku_sales = dict()
            sku_costs = dict()
            for key in list(dict_to_search.keys())[:-1]:
                with open(dict_to_search[key], 'r') as csvfile:
                    reader = csv.DictReader(csvfile, delimiter='|')
                    for row in reader:
                        par_key = '{}-{}'.format(row['SKU'].strip(), row['NRO_LOCAL'])
                        sku_sales[par_key] = float(row['VENTA_{}'.format(self.MONEDA)])
                        if 'COSTO' in row:
                            sku_costs[par_key] = float(parse_decimal(row['COSTO'], locale='de'))

            with open(file_name, 'r') as csvfile:
                reader = csv.DictReader(csvfile, delimiter='|')
                day = self.WEEK_DAYS_FOR_DAILY_CASE[self._date.weekday()]
                venta_str = day + self._date.strftime('_%d_%m')
                for row in reader:
                    new_row = dict()
                    for key in row.keys():
                        new_row[key] = row[key].strip()
                    if row['SKU'].strip() in product_dict:
                        for key in product_dict[row['SKU'].strip()].keys():
                            new_row[key] = product_dict[row['SKU'].strip()][key]
                    else:
                        new_row['DESC_LINEA'] = ''
                        new_row['DESC_CLASE'] = ''
                    new_row['Datetime'] = self._date.strftime('%Y-%m-%d %H:%M:%S 00:00')

                    if '#' in row[venta_str]:
                        logger.debug("Skipping row {}".format(row))
                        continue

                    venta_unidades = float(parse_decimal(row['VENTA_UNIDADES'], locale='en_US'))
                    venta_pesos = float(parse_decimal(row['VENTA_{}'.format(self.MONEDA)], locale='en_US'))
                    costo = float(parse_decimal(row['COSTO'], locale='en_US')) if 'COSTO' in row else None
                    dia_venta = float(row[venta_str])

                    if dia_venta:
                        par_key = '{}-{}'.format(row['SKU'].strip(), row['NRO_LOCAL'])
                        monto_venta = venta_pesos - sku_sales[par_key] if par_key in sku_sales else venta_pesos
                        costo_venta = costo - sku_costs[par_key] if par_key in sku_costs else costo
                    else:
                        monto_venta = 0
                        costo_venta = 0 if 'COSTO' in row else None

                    new_row['VENTA_UNIDAD'] = venta_unidades
                    new_row['MONTO_VENTA'] = monto_venta
                    new_row['COSTO_VENTA'] = costo_venta
                    new_row['VENTA_UNIDAD_DIA'] = dia_venta
                    if 'SUBCLASE' not in new_row:
                        new_row['SUBCLASE'] = new_row['SUBCLASE-CONJUNTO']
                    if 'DESC_SUBCLASE' not in new_row:
                        new_row['DESC_SUBCLASE'] = new_row['DESC_SUBCLASE-DESC_CONJUNTO']
                    data.append(new_row)
        else:
            date_from = datetime.datetime.strptime(file_name.split('_')[-2], '%Y-%m-%d')
            with open(file_name, 'r', encoding='latin1') as csvfile:
                reader = csv.DictReader(csvfile, delimiter='|')
                for row in reader:
                    new_row = dict()
                    string_days = list()
                    for key in row.keys():
                        new_row[key] = row[key].strip()
                    if row['SKU'].strip() in product_dict:
                        for key in product_dict[row['SKU'].strip()].keys():
                            new_row[key] = product_dict[row['SKU'].strip()][key]
                    else:
                        new_row['DESC_LINEA'] = ''
                        new_row['DESC_CLASE'] = ''
                    new_row['Datetime'] = self._date.strftime('%Y-%m-%d %H:%M:%S 00:00')

                    venta_pesos = float(parse_decimal(new_row['VENTA_{}'.format(self.MONEDA)], locale='en_US'))
                    venta_unidades = float(parse_decimal(new_row['VENTA_UNIDADES'], locale='en_US'))
                    costo = float(parse_decimal(new_row['COSTO'], locale='en_US')) if 'COSTO' in new_row else None

                    for i in range(0, 7):
                        dt = date_from - datetime.timedelta(days=i)
                        day = self.WEEK_DAYS_FOR_WEEKLY_CASE[dt.weekday()]
                        venta_str = day + dt.strftime("_%d-%m")

                        dia_venta = float(new_row[venta_str])
                        new_row['Datetime_' + venta_str] = dt

                        if venta_unidades != 0:
                            monto_venta = (venta_pesos / venta_unidades) * dia_venta
                            costo_venta = (costo / venta_unidades) * dia_venta if costo else None
                        else:
                            monto_venta = 0
                            costo_venta = 0 if costo else None

                        new_row['MONTO_VENTA_' + venta_str] = monto_venta
                        new_row['COSTO_VENTA_' + venta_str] = costo_venta
                        new_row['VENTA_UNIDAD_' + venta_str] = dia_venta

                        string_days.append(venta_str)

                    new_row['VENTA_UNIDAD'] = venta_unidades
                    new_row['STRING_DAYS'] = string_days
                    if 'SUBCLASE' not in new_row:
                        new_row['SUBCLASE'] = new_row['SUBCLASE-CONJUNTO']
                    if 'DESC_SUBCLASE' not in new_row:
                        new_row['DESC_SUBCLASE'] = new_row['DESC_SUBCLASE-DESC_CONJUNTO']
                    data.append(new_row)

        return data


class FalabellaB2BStockConnector(FalabellaB2BFileBaseConnector):

    def detalle_venta(self):

        filesearch_result = self.find_file('ventas')
        if not filesearch_result:
            return []

        file_name, ts_file, daily_file = filesearch_result

        product_dict = self.get_product_values()

        data = list()

        with open(file_name, 'r') as csvfile:
            reader = csv.DictReader(csvfile, delimiter='|')
            for row in reader:
                new_row = dict()
                for key in row.keys():
                    new_row[key] = row[key].strip()
                if new_row['SKU'] in product_dict:
                    for key in product_dict[new_row['SKU']].keys():
                        new_row[key] = product_dict[new_row['SKU']][key].strip()
                else:
                    new_row['DESC_LINEA'] = ''
                    new_row['DESC_CLASE'] = ''
                    new_row['PRECIO_VTA'] = 0
                new_row['Datetime'] = self._date.strftime('%Y-%m-%d %H:%M:%S 00:00')

                if 'STOCK' in row:
                    stock = float(row['STOCK'])
                elif 'STOCK_DISPONIBLE' in row:
                    stock = float(row['STOCK_DISPONIBLE'])
                else:
                    logger.error("No Stock column in the file")
                    raise ValueError
                stock_valor = float(parse_decimal(str(new_row['PRECIO_VTA']), locale='en_US')) * stock

                new_row['STOCK'] = stock
                new_row['STOCK_VALOR'] = stock_valor

                if 'SUBCLASE' not in new_row:
                    new_row['SUBCLASE'] = new_row['SUBCLASE-CONJUNTO']
                if 'DESC_SUBCLASE' not in new_row:
                    new_row['DESC_SUBCLASE'] = new_row['DESC_SUBCLASE-DESC_CONJUNTO']

                data.append(new_row)

        return data


class FalabellaB2BPeruPortalConnector(FalabellaB2BPortalConnector):
    BASE_URL = 'https://b2b.falabella.com/b2bprdpesa/'

    CADENA = "4"
    DATE_STYLE = "width:9%;text-align:center"


class FalabellaB2BPeruFileConnector(FalabellaB2BFileConnector):

    MONEDA = "SOLES"
