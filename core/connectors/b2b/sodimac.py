import re
import datetime
import os
import glob
import csv
from babel.numbers import parse_decimal
from collections import Counter
from itertools import repeat, chain
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

from core.connectors.b2b.base import B2BConnector
from core.connectors.b2b.utils import ConnectorB2BLoginErrorConnection
from utils.logger import logger

from selenium import webdriver
from pyvirtualdisplay import Display
from selenium.webdriver.support.ui import Select

import time


class SodimacB2BPortalConnector(B2BConnector):
    BASE_URL = 'https://b2b.sodimac.com/b2bsoclpr/'
    HOME_PATH = 'grafica/html/main_home.html'
    LOGIN_PATH = 'logica/jsp/B2BBF000.jsp?MODULO=4'
    INFORMES_PATH = 'logica/jsp/B2BvFDescarga.do?tipo=eVTA'
    SIGUIENTE_INFORMES = 'logica/jsp/B2BvFDescarga.do?d-16544-p={}&tipo=eVTA'
    INFORME_PRODUCTOS = 'logica/jsp/B2BvFDescarga.do?tipo=eCAT'
    LOGOUT_PATH = 'logica/jsp/B2BvFCerrarSesion.jsp'
    HISTORICAL_DATA_REQUEST = 'logica/jsp/B2BvFParametrosVentaHistorico.do'
    HISTORICAL_GET_PARAMS = '?d-16544-p={}&accion=inicio'

    CADENA = "6"

    PORTAL = 'Sodimac'

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

    def login(self, driver):
        self.check_login_credentials(self.b2b_username, self.b2b_password)

        url_home = self.BASE_URL + self.HOME_PATH

        driver.get(url_home)
        driver.refresh()
        time.sleep(3)
        select = Select(driver.find_element_by_id("CADENA"))
        select.select_by_value("6")
        driver.find_element_by_id("empresa").send_keys(self.b2b_empresa)
        driver.find_element_by_id("usuario").send_keys(self.b2b_username)
        driver.find_element_by_id("clave").send_keys(self.b2b_password)
        driver.find_element_by_id("entrar2").click()
        if driver.current_url != url_home:
            return True
        else:
            arg = {
                'username': self.b2b_username,
                'portal': 'Sodimac'
            }
            raise ConnectorB2BLoginErrorConnection(arg)

    def logout(self, driver):
        url_logout = self.BASE_URL + self.LOGOUT_PATH
        driver.get(url_logout)

    def create_actual_sale_files(self, driver, **kwargs):
        periods_checked = list()

        url_informe = self.BASE_URL + self.INFORMES_PATH
        driver.get(url_informe)

        items_text_list = driver.find_elements_by_class_name("displayTagTablapagebanner")
        if items_text_list:
            items_text = items_text_list[0]
        else:
            logger.debug("No Actual Files Availables to Download")
            return periods_checked

        items_amount = int(re.search(r'\d+', items_text.text).group())

        self.by_week = items_amount < 6

        page = 1
        url_files = list()
        while items_amount > 0:
            elems = driver.find_elements_by_xpath("//a[@href]")
            for elem in elems:
                href_attr = elem.get_attribute("href")
                if '.txt' in href_attr and 'crdownload' not in href_attr:
                    url_files.append(href_attr)
            items_amount -= 15
            page += 1
            next_url = self.BASE_URL + self.SIGUIENTE_INFORMES.format(page)
            driver.get(next_url)

        today_date = datetime.datetime.today()
        for url_sales_file in url_files:
            driver.get(url_sales_file)
            time.sleep(2)

        raw_folder = os.path.join(kwargs['source_int_path'], 'b2b-files/Sodimac/raw/*')
        for name in glob.glob(raw_folder):
            filename_date_text = name.split('-')[-1].replace('.txt', '').split(' ')[0]
            logger.debug("Reading file {}".format(name))
            filename_date_datetime = datetime.datetime.strptime(filename_date_text, '%Y%m%d')

            with open(name, 'r') as csvfile:
                reader = csv.DictReader(csvfile, delimiter='|')
                headers = reader.fieldnames

            days_in_file = list()
            if not headers:
                logger.debug("Malformed file {}".format(name))
                os.remove(name)
                continue

            for h in headers:
                if self.by_week:
                    for day_of_week in self.WEEK_DAYS_FOR_WEEKLY_CASE:
                        if day_of_week in h:
                            days_in_file.append(h)
                else:
                    for day_of_week in self.WEEK_DAYS_FOR_DAILY_CASE:
                        if day_of_week in h:
                            days_in_file.append(h)

            date_format_filename = '%d-%m'

            date_t = (filename_date_datetime - datetime.timedelta(days=1)).strftime('%Y-') + datetime.datetime.strptime(days_in_file[-1].split('_', 1)[1], date_format_filename).strftime('%m-%d')

            if datetime.datetime.strptime(date_t, '%Y-%m-%d') > datetime.datetime.today():
                date_t_datetime = datetime.datetime.strptime(date_t, '%Y-%m-%d')
                date_t = date_t_datetime.replace(year=date_t_datetime.year - 1).strftime('%Y-%m-%d')

            if len(days_in_file) == 1:
                date_f = date_t

            elif len(days_in_file) > 1:
                date_f = (datetime.datetime.strptime(date_t, '%Y-%m-%d') - datetime.timedelta(days=6)).strftime('%Y-%m-%d')

            else:
                logger.error("Cannot calculate days of the data")
                continue

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

            logger.debug("Renaming {} file to {}".format(name, file_full_path))
            os.rename(name, file_full_path)
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

    def create_product_file(self, driver, **kwargs):
        url_informe_products = self.BASE_URL + self.INFORME_PRODUCTOS
        today_date = datetime.datetime.today()

        driver.get(url_informe_products)
        download_path = driver.find_element_by_class_name("tablaDatos").find_element_by_tag_name('a').get_attribute('href')
        driver.get(download_path)
        time.sleep(2)
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

        raw_folder = os.path.join(kwargs['source_int_path'], 'b2b-files/Sodimac/raw/*')
        for name in glob.glob(raw_folder):
            logger.debug("Saving file {}".format(file_full_path))
            os.rename(name, file_full_path)

    def create_historical_sale_files(self, driver, page, **kwargs):
        periods_checked = list()
        today_date = datetime.datetime.today()

        url_historical_files = self.BASE_URL + self.HISTORICAL_DATA_REQUEST + self.HISTORICAL_GET_PARAMS
        url_files_to_download = list()
        driver.get(url_historical_files.format(page))
        elems = driver.find_elements_by_xpath("//a[@href]")
        if_proximo = False
        for elem in elems:
            if '.txt' in elem.get_attribute("href"):
                url_files_to_download.append(elem.get_attribute("href"))
            if 'Próximo' in elem.text:
                if_proximo = True

        if not url_files_to_download:
            return periods_checked

        raw_folder = os.path.join(kwargs['source_int_path'], 'b2b-files/Sodimac/raw/')

        for url_file in url_files_to_download:
            filename = url_file.split('/')[-1]
            filename_date_text = url_file.split('-')[-1].replace('.txt', '')
            filename_date_datetime = datetime.datetime.strptime(filename_date_text, '%Y%m%d')
            date_t = filename_date_datetime.strftime('%Y-%m-%d')
            date_f = (filename_date_datetime.replace(day=1)).strftime('%Y-%m-%d')
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
            driver.get(url_file)
            time.sleep(5)
            file_downloaded_path = os.path.join(raw_folder, filename)
            logger.debug("Saving file {}".format(file_full_path))
            os.rename(file_downloaded_path, file_full_path)
            periods_checked.append({
                'from': date_f,
                'to': date_t,
                'status': 'ok',
            })

        if if_proximo and page != 15:
            return periods_checked + self.create_historical_sale_files(driver, page + 1, **kwargs)

        return periods_checked

    def check_periods_requested(self, driver, page, **kwargs):
        period_requested = []
        url_historical_files = self.BASE_URL + self.HISTORICAL_DATA_REQUEST + self.HISTORICAL_GET_PARAMS
        driver.get(url_historical_files.format(page))
        response = driver.page_source
        soup = BeautifulSoup(response, 'html.parser')
        tablas = soup.findAll(class_='tablaDatos')
        for tabla in tablas:
            if tabla.find('th', class_='order1'):
                for row in tabla.findAll('td'):
                    if '/' in row.text:
                        period_requested.append(row.text)
        if soup.find('a', text='Próximo', href=True) and page != 15:
            return period_requested + self.check_periods_requested(driver, page + 1, **kwargs)
        return period_requested

    def request_month(self, driver, date):
        logger.debug('Request historical data to date {}'.format(date))
        url_request_historical_data = self.BASE_URL + self.HISTORICAL_DATA_REQUEST
        driver.get(url_request_historical_data)
        select = Select(driver.find_element_by_name("anno"))
        select.select_by_value(date.split('-')[0])
        select2 = Select(driver.find_element_by_name("mes"))
        select2.select_by_value(date.split('-')[1])
        driver.find_element_by_name("botonBuscar").click()

        if driver.find_elements_by_class_name("texto-alert"):
            logger.debug(driver.find_element_by_class_name("texto-alert").text)

    def generate_files(self, **kwargs):
        display = Display(visible=0)
        display.start()

        from selenium.webdriver.chrome.options import Options

        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_experimental_option("prefs", {
            "download.default_directory": os.path.join(kwargs['source_int_path'], 'b2b-files/Sodimac/raw'),
            "download.prompt_form_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })

        driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(),
                                  service_args=['--verbose', '--log-path=/tmp/chromedriver.log'], chrome_options=options)

        today_date = datetime.datetime.today()

        self.by_week = False

        self.login(driver)

        periods_checked = self.create_actual_sale_files(driver, **kwargs)

        self.create_product_file(driver, **kwargs)

        periods_checked += self.create_historical_sale_files(driver, 1, **kwargs)

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
        periods_requested = self.check_periods_requested(driver, 1, **kwargs)

        for requested in periods_requested:
            logged = False
            file_period_from = datetime.datetime.strptime(requested, '%d/%m/%Y')
            file_period_from = file_period_from.replace(day=1)
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
        while len(periods_requested) < 2 and 1 <= len(result):
            self.request_month(driver, result[0])
            del(result[0])
            periods_requested = self.check_periods_requested(driver, 1, **kwargs)

        self.logout(driver)

        driver.quit()
        display.stop()

        return periods_checked


class SodimacB2BFileBaseConnector(B2BConnector):

    file_pattern = '{filetype}_{b2b_username}_{b2b_empresa}_*.csv'
    file_pattern_with_date = '{filetype}_{b2b_username}_{b2b_empresa}_{fecha}_{fecha}_*.csv'
    file_pattern_final_date = '{filetype}_{b2b_username}_{b2b_empresa}_*_{fecha}_*.csv'
    fixed_sub_folder = 'Sodimac'
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


class SodimacFileConnector(SodimacB2BFileBaseConnector):

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
                    is_historical_file = 'STOCK' in row.keys()
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

                    if is_historical_file:
                        for i in range(0, 31):
                            dt = date_from - datetime.timedelta(days=i)
                            venta_str = dt.strftime("%d-%-m-%Y")
                            if venta_str in new_row:
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

                    else:

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


class SodimacB2BStockConnector(SodimacB2BFileBaseConnector):

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
                elif 'STOCK_CONTABLE_FISICO' in row:
                    stock = float(row['STOCK_CONTABLE_FISICO'])
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
