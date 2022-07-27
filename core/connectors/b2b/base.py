import ast
import csv
import datetime
import glob
import json
import os
import time
import zipfile

import pandas as pd

from bs4 import BeautifulSoup
from requests_html import HTMLSession
from urllib3.exceptions import ProtocolError

from selenium.webdriver.common.keys import Keys
from seleniumwire import webdriver
from pyvirtualdisplay import Display

from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

from difflib import SequenceMatcher as SM

from webdriver_manager.chrome import ChromeDriverManager

from core.connectors import Connector
from core.connectors.b2b.utils import ConnectorB2BClientInPortalError
from core.connectors.b2b.utils import ConnectorB2BGenericError
from core.connectors.b2b.utils import ConnectorB2BLoginErrorConnection
from core.connectors.b2b.utils import do_request
from core.connectors.b2b.utils import NotCredentialsProvidedError
from core.connectors.b2b.utils import SaveErrorLogWhenBadCredentials
from utils.logger import logger
from utils.recaptcha import resolve_recaptcha


class B2BConnector(Connector):
    def detalle_venta(self):
        raise NotImplementedError

    def check_if_csv(self, path, delimiter=',', encoding='utf-8'):
        try:
            pd.read_csv(path, sep=delimiter, encoding=encoding)
        except Exception:
            return False
        return True

    def check_login_credentials(self, username, password):
        if not username and not password:
            raise NotCredentialsProvidedError


class BBReCommercePortalConnector(B2BConnector):
    SELECTOR_PORTAL_URL = 'https://www.cenconlineb2b.com/'
    POST_URL = 'https://www.cenconlineb2b.com/ParisCL/BBRe-commerce/main/UIDL/?v-uiId=0'

    FILE_NAME_PATH = 'b2b-files/{portal}/ventas/{year}/{month}/{filetype}_{client}_{empresa}_{date_from}_{date_to}_{timestamp}.csv'

    UNIQUE_PARAMS = None

    ENCODING = 'latin1'

    PORTAL = 'Undefined'
    PERIODO_COLUMN_NAME = 'PERIODO'

    @classmethod
    def get_instance(cls, **kwargs):
        b2b_username = kwargs['b2b_username']
        b2b_password = kwargs['b2b_password']
        b2b_empresa = kwargs['b2b_empresa']

        return cls(b2b_username, b2b_password, b2b_empresa)

    def __init__(self,
                 b2b_username,
                 b2b_password,
                 b2b_empresa='',
                 ):

        self.b2b_username = b2b_username
        self.b2b_password = b2b_password
        self.b2b_empresa = b2b_empresa

    def login_form(self, driver):
        driver.get(self.SELECTOR_PORTAL_URL)
        driver.find_element_by_xpath('//*[@id="pais"]/option[3]').click()
        driver.find_element_by_xpath(self.UNIQUE_PARAMS['unidad_negocio']).click()
        driver.find_element_by_id('btnIngresar').click()
        time.sleep(4)

    def login(self, driver):
        self.check_login_credentials(self.b2b_username, self.b2b_password)

        self.login_form(driver)

        driver.find_element_by_id('username').send_keys(self.b2b_username)
        driver.find_element_by_id('password').send_keys(self.b2b_password)
        sitekey = driver.find_element_by_class_name('g-recaptcha').get_attribute('data-sitekey')
        result = resolve_recaptcha(sitekey, driver.current_url)
        driver.execute_script(f'document.getElementById("g-recaptcha-response").value = "{ result }"')
        driver.find_element_by_id("kc-login").click()
        logger.debug("Waiting portal to login")
        time.sleep(20)

        key_request = None
        for request in driver.requests:
            if 'main?v-' in request.path:
                key_request = request

        if not key_request:
            arg = {
                'username': self.b2b_username,
                'portal': self.PORTAL
            }
            raise ConnectorB2BLoginErrorConnection(arg)

    def get_relative_span(self, spans, **kwargs):
        text_to_search = kwargs.get('b2b_empresa')
        if not text_to_search:
            return None
        elements = dict()
        for span in spans:
            elements[span.text.lower()] = span
        matches = sorted(list(elements.keys()), key=lambda x: SM(None, x, text_to_search.lower()).ratio(), reverse=True)
        return elements[matches[0]] if matches else None

    def download_clicks(self, driver):
        driver.find_element_by_xpath(self.UNIQUE_PARAMS['Download Click 1']).click()
        time.sleep(10)
        driver.find_element_by_xpath(self.UNIQUE_PARAMS['Download Click 2']).click()
        time.sleep(4)
        driver.find_element_by_xpath(self.UNIQUE_PARAMS['Download Click 3']).click()
        time.sleep(30)

    def extra_extraction(self, driver):
        pass

    def save_file(self, file, today_date, **kwargs):
        with open(file, 'r', encoding=self.ENCODING) as csvfile:
            csvreader = csv.DictReader(x.replace('\0', '') for x in csvfile)
            for row in csvreader:
                file_date = row[self.PERIODO_COLUMN_NAME].split(' al ')[0]
                continue
        file_name = self.FILE_NAME_PATH.format(
            portal=self.PORTAL,
            year=today_date.strftime('%Y'),
            month=today_date.strftime('%m'),
            filetype='ventas',
            client=self.b2b_username,
            empresa=self.b2b_empresa,
            date_from=file_date,
            date_to=file_date,
            timestamp=today_date.timestamp()
        )

        file_full_path = os.path.join(kwargs['source_int_path'], file_name)
        os.makedirs(os.path.dirname(file_full_path), exist_ok=True)
        logger.debug("Saving file {}".format(file_full_path))
        os.rename(file, file_full_path)

    def check_stock_box(self, driver):
        pass

    def generate_files(self, **kwargs):
        periods_checked = list()
        download_folder = os.path.join(kwargs['source_int_path'], f'b2b-files/{self.PORTAL}/raw')
        url_requests = self.POST_URL

        if not self.UNIQUE_PARAMS:
            logger.error('UNIQUE_PARAMS not defined')
            raise ConnectorB2BGenericError

        display = Display(visible=not bool(os.environ.get('WIVO_ENV', False)))
        display.start()
        try:
            options = webdriver.ChromeOptions()
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_experimental_option("prefs", {
                "download.default_directory": download_folder,
                "download.prompt_from_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True
            })

            driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(),
                                      service_args=['--verbose', '--log-path=/tmp/chromedriver.log'],
                                      chrome_options=options,
                                      seleniumwire_options={
                                          'verify_ssl': False,
                                          'connection_timeout': None})

            self.login(driver)

            driver.find_element_by_tag_name('html').send_keys(Keys.CONTROL, Keys.SUBTRACT)

            date_format = '%Y-%m-%d'
            date_from = datetime.datetime.strptime(kwargs['from'], date_format)
            date_to = datetime.datetime.strptime(kwargs['to'], date_format)

            for popup_xpath in self.UNIQUE_PARAMS['popup_messages']:
                if driver.find_elements_by_xpath(popup_xpath):
                    driver.find_element_by_xpath(popup_xpath).click()
                    time.sleep(1)

            kwargs['file_downloaded'] = dict()

            try:
                while date_from <= date_to:
                    # Press in 'Comercial' button
                    logger.debug('Changing to Comercial Menu')

                    time.sleep(1)
                    driver.find_element_by_xpath(self.UNIQUE_PARAMS['Comercial Menu']).click()
                    time.sleep(2)
                    driver.find_element_by_xpath(self.UNIQUE_PARAMS['Informe de Ventas']).click()
                    time.sleep(20)

                    self.check_stock_box(driver)

                    if len(driver.find_elements_by_xpath('/html/body/div[2]/div[5]/div/div/div[2]/div[2]')):
                        driver.find_element_by_xpath('/html/body/div[2]/div[5]/div/div/div[2]/div[2]').click()
                        time.sleep(5)

                    if len(driver.find_elements_by_xpath('/html/body/div[2]/div[3]/div/div/div[2]/div[2]')):
                        driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/div/div[2]/div[2]').click()
                        time.sleep(5)

                    if len(driver.find_elements_by_xpath('/html/body/div[2]/div[3]/div/div/div[3]/div/div/div[1]/div/div/div[3]/div')):
                        if 'carga' in driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/div/div[3]/div/div/div[1]/div/div/div[3]/div').text:
                            logger.warning('El módulo de ventas se encuentra en proceso de carga. Esperar unos minutos.')

                    if kwargs.get('b2b_empresa'):
                        driver.find_element_by_xpath(self.UNIQUE_PARAMS['Empresa Selector']).click()
                        time.sleep(2)
                        spans = driver.find_element_by_xpath(self.UNIQUE_PARAMS['Span Options']).find_elements_by_tag_name('span')
                        span = self.get_relative_span(spans, **kwargs)
                        if span:
                            span.click()

                    logger.debug(f'Changing period to {date_from:%Y-%m-%d}')
                    driver.find_element_by_xpath(self.UNIQUE_PARAMS['Calendar From']).click()
                    driver.find_element_by_id('PID_VAADIN_POPUPCAL-1-0').click()
                    time.sleep(2)
                    driver.find_element_by_xpath(self.UNIQUE_PARAMS['Calendar From']).click()
                    driver.find_element_by_id('PID_VAADIN_POPUPCAL-1-1').click()
                    time.sleep(2)
                    driver.find_element_by_xpath(self.UNIQUE_PARAMS['Calendar From']).click()
                    driver.find_element_by_id('PID_VAADIN_POPUPCAL-1-0').click()
                    time.sleep(2)

                    last_request = None
                    for request in driver.requests:
                        if 'UIDL/?v-uiId=0' in request.path:
                            last_request = request

                    headers_for_requests = ''
                    for key in last_request.headers.keys():
                        headers_for_requests += f"xhr.setRequestHeader('{key}', '{last_request.headers[key]}'); "

                    body_json = json.loads(last_request.body.decode('utf-8'))
                    body_json['syncId'] += 1
                    body_json['clientId'] += 1
                    body_json['rpc'][0][3][1]['YEAR'] = date_from.year
                    body_json['rpc'][0][3][1]['MONTH'] = date_from.month
                    body_json['rpc'][0][3][1]['DAY'] = date_from.day

                    js = f'''var xhr = new XMLHttpRequest();
                    xhr.open('POST', '{url_requests}', false);
                    {headers_for_requests}
                    xhr.send('{json.dumps(body_json)}');
                    return xhr.response;'''

                    driver.execute_script(js)

                    time.sleep(2)

                    driver.find_element_by_xpath(self.UNIQUE_PARAMS['Calendar To']).click()
                    driver.find_element_by_id('PID_VAADIN_POPUPCAL-1-0').click()
                    time.sleep(2)
                    driver.find_element_by_xpath(self.UNIQUE_PARAMS['Calendar To']).click()
                    driver.find_element_by_id('PID_VAADIN_POPUPCAL-1-1').click()
                    time.sleep(2)
                    driver.find_element_by_xpath(self.UNIQUE_PARAMS['Calendar To']).click()
                    driver.find_element_by_id('PID_VAADIN_POPUPCAL-1-0').click()
                    time.sleep(2)

                    headers_for_requests = ''
                    for key in driver.last_request.headers.keys():
                        headers_for_requests += f"xhr.setRequestHeader('{key}', '{driver.last_request.headers[key]}'); "

                    body_json = json.loads(driver.last_request.body.decode('utf-8'))

                    if 'syncId' not in body_json:
                        continue

                    body_json['syncId'] += 1
                    body_json['clientId'] += 1
                    body_json['rpc'][0][3][1]['YEAR'] = date_from.year
                    body_json['rpc'][0][3][1]['MONTH'] = date_from.month
                    body_json['rpc'][0][3][1]['DAY'] = date_from.day

                    js = f'''var xhr = new XMLHttpRequest();
                                xhr.open('POST', '{url_requests}', false);
                                {headers_for_requests}
                                xhr.send('{json.dumps(body_json)}');
                                return xhr.response;'''

                    driver.execute_script(js)

                    time.sleep(2)

                    driver.find_element_by_xpath(self.UNIQUE_PARAMS['Empresa Selector']).click()
                    time.sleep(2)
                    driver.find_element_by_xpath(self.UNIQUE_PARAMS['Estacion Selector']).click()
                    time.sleep(2)
                    driver.find_element_by_xpath(self.UNIQUE_PARAMS['Empresa Selector']).click()
                    time.sleep(3)

                    logger.debug(f'Requesting period {date_from:%Y-%m-%d}')

                    input_parameter1 = "/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[4]/div/div/div[3]/div/div[3]/div/div[1]/div/div/div[1]/span"
                    input_parameter2 = "/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[4]/div/div/div[3]/div/div[3]/div/div[2]/div/div/div[1]/span"

                    WebDriverWait(driver, 10).until(
                        expected_conditions.element_to_be_clickable((By.XPATH, input_parameter1))).click()
                    WebDriverWait(driver, 10).until(
                        expected_conditions.element_to_be_clickable((By.XPATH, input_parameter2))).click()

                    search_button = driver.find_element_by_xpath(self.UNIQUE_PARAMS['Search Button'])
                    search_button.click()
                    time.sleep(30)

                    self.download_clicks(driver)

                    request_with_url = None
                    for request in driver.requests:
                        if request.response and request.response.body and 'Ventas' in request.response.body.decode('latin-1'):
                            request_with_url = request

                    url = request_with_url.response.body.decode('latin-1').split('"uRL":"')[1].split('"')[0]
                    logger.debug(f"Making request to {url}")
                    driver.get(url)

                    time.sleep(1)
                    download_popup = driver.find_element_by_xpath(self.UNIQUE_PARAMS['Close Download Popup'])
                    download_popup.click()
                    time.sleep(2)

                    periods_checked.append({
                        'from': date_from.strftime(date_format),
                        'to': date_from.strftime(date_format),
                        'status': 'ok'
                    })

                    kwargs['file_downloaded'][url] = date_from.strftime(date_format)

                    date_from += datetime.timedelta(days=1)

                self.extra_extraction(driver)

                WebDriverWait(driver, 10).until(
                    expected_conditions.element_to_be_clickable((By.XPATH,
                                                                 self.UNIQUE_PARAMS['Logout Click 1']))).click()
                WebDriverWait(driver, 10).until(
                    expected_conditions.element_to_be_clickable((By.XPATH,
                                                                 self.UNIQUE_PARAMS['Logout Click 2']))).click()
            except Exception as e:
                logger.debug("There's an error downloading data")
                logger.debug(e)
        except Exception as e:
            logger.debug("the execution stopped abruptly")
            logger.debug(e)
        finally:
            display.sendstop()

        for file in glob.glob(os.path.join(download_folder, '*.zip')):
            with zipfile.ZipFile(file, 'r') as zip_ref:
                zip_ref.extractall(download_folder)
            os.remove(file)

        today_date = datetime.datetime.today()
        for file in glob.glob(os.path.join(download_folder, '*.csv')):
            self.save_file(file, today_date, **kwargs)

        return periods_checked


class BBReCommerceFileBaseConnector(B2BConnector):

    file_pattern = '{filetype}_{b2b_username}_{b2b_empresa}_*.csv'
    fixed_sub_folder = ''
    fixed_sub_root_folder = 'b2b-files'
    fixed_sub_sales_folder = 'ventas'

    ENCODE = 'ISO-8859-15'

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
        self.user_name = b2b_username.replace('_', '')
        self.b2b_empresa = b2b_empresa
        self.repository_path = os.path.join(
            base_path, self.fixed_sub_root_folder, self.fixed_sub_folder, self.fixed_sub_sales_folder)

    def find_file(self, filetype, per_timestamp=False, last_file=False):
        file_name = self.file_pattern.format(filetype=filetype, b2b_username=self.user_name, b2b_empresa=self.b2b_empresa)
        file_path = os.path.join(self.repository_path, '**', file_name)
        files = glob.glob(file_path, recursive=True)
        file_name = None
        ts_file = 0
        for filename in files:
            timestamp_for_file = float(filename.split('_')[-1].split('.csv')[0])
            if not per_timestamp:
                date_from = datetime.datetime.strptime(filename.split('_')[3], '%Y-%m-%d').date()
                date_to = datetime.datetime.strptime(filename.split('_')[4], '%Y-%m-%d').date()
            else:
                date_from = datetime.date.fromtimestamp(timestamp_for_file)
                date_to = datetime.date.fromtimestamp(timestamp_for_file)
            if last_file or (date_from <= self._date <= date_to):
                logger.debug("File whit data {}".format(filename))
                if ts_file < timestamp_for_file:
                    file_name = filename
                    ts_file = timestamp_for_file
        if file_name:
            logger.debug("File to process {}".format(file_name))
            return (file_name, ts_file)
        else:
            logger.debug('No files to process {}'.format(file_path))
            return (None, None)

    def detalle_venta(self):
        data = []
        file_name, ts_file = self.get_file_name()

        if file_name:
            with open(file_name, 'r', encoding=self.ENCODE) as csvfile:
                reader = csv.DictReader(x.replace('\0', '') for x in csvfile)
                for row in reader:
                    new_row = row
                    new_row['Datetime'] = self.date_start
                    self.parse_metrics(new_row)
                    data.append(new_row)
        return self.complement_data(data)

    def get_file_name(self):
        raise NotImplementedError

    def parse_metrics(self, row):
        raise NotImplementedError

    def complement_data(self, data):
        return data


class BBReCommerceFileConnector(BBReCommerceFileBaseConnector):

    def get_file_name(self):
        return self.find_file('ventas')


class BBReCommerceStockFileConnector(BBReCommerceFileBaseConnector):

    def get_file_name(self):
        return self.find_file('ventas', per_timestamp=True)


class EmpresasSBB2BPortalConnector(B2BConnector):
    BASE_URL = 'https://portalempresas.sb.cl'
    LOGIN_PATH = '/login.php'
    CAPTCHA_PATH = '/captcha/captcha-request.php'
    VERIFY_LOGIN_PATH = '/php/validaLogin.php'
    INDEX_PATH = '/index.php'
    REDIRECTION_PATH = '/b2b/src/controladores/seguridad/redireccion.php'
    VALIDAMANTENEDOR_PATH = '/b2b/src/controladores/seguridad/validacionmantenedor.php'
    SOURCE_URL_PATH = '/b2b/web/inicio.php'
    MANTENEDOR_PATH = '/b2b/web/mantenedor.html'
    CONTROLADORES_PATH = '/b2b/src/controladores'
    LOGOUT_PATH = '/logout.php'

    SOURCE_NAME = ''

    RAW_DATA_FOLDER = os.path.join(os.environ['SOURCE_INT_PATH'], 'raw_data', SOURCE_NAME)

    FILE_NAME_PATH = 'b2b-files/{portal}/ventas/{year}/{month}/{filetype}_{client}_{empresa}_{date_from}_{date_to}_{timestamp}.csv'

    @classmethod
    def get_instance(cls, **kwargs):
        b2b_username = kwargs['b2b_username']
        b2b_password = kwargs['b2b_password']
        return cls(b2b_username, b2b_password)

    def __init__(self, b2b_username, b2b_password):
        self.b2b_username = b2b_username
        self.b2b_password = b2b_password
        self.fieldnames = []

    def create_lab_file(self, labs, **kwargs):
        today_date = datetime.datetime.today()
        filename = self.FILE_NAME_PATH.format(
            portal=self.SOURCE_NAME,
            year=today_date.strftime('%Y'),
            month=today_date.strftime('%m'),
            filetype='labs',
            client=kwargs['b2b_username'],
            empresa=self.SOURCE_NAME,
            date_from=kwargs['from'],
            date_to=kwargs['to'],
            timestamp=today_date.timestamp(),
        )
        file_full_path = os.path.join(kwargs['source_int_path'], filename)
        os.makedirs(os.path.dirname(file_full_path), exist_ok=True)
        with open(file_full_path, 'w') as csvfile:
            logger.debug('Making file {}'.format(file_full_path))
            writer = csv.DictWriter(csvfile, fieldnames=['lab'])
            writer.writeheader()
            for lab in labs:
                writer.writerow({'lab': lab})

    def create_product_file(self, products, **kwargs):
        today_date = datetime.datetime.today()
        filename = self.FILE_NAME_PATH.format(
            portal=self.SOURCE_NAME,
            year=today_date.strftime('%Y'),
            month=today_date.strftime('%m'),
            filetype='products',
            client=kwargs['b2b_username'],
            empresa=self.SOURCE_NAME,
            date_from=kwargs['from'],
            date_to=kwargs['to'],
            timestamp=today_date.timestamp(),
        )
        file_full_path = os.path.join(kwargs['source_int_path'], filename)
        os.makedirs(os.path.dirname(file_full_path), exist_ok=True)
        writer = None
        with open(file_full_path, 'w') as csvfile:
            logger.debug('Making file {}'.format(file_full_path))
            fieldnames = False
            for sku in products.keys():
                if not fieldnames:
                    fieldnames = products[sku].keys()
                    fieldnames = ['Sku'] + list(fieldnames)
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                sku_dict = {'Sku': sku}
                writer.writerow({**sku_dict, **products[sku]})

    def create_quiebre_file(self, products, **kwargs):
        today_date = datetime.datetime.today()
        filename = self.FILE_NAME_PATH.format(
            portal=self.SOURCE_NAME,
            year=today_date.strftime('%Y'),
            month=today_date.strftime('%m'),
            filetype='quiebre',
            client=kwargs['b2b_username'],
            empresa=self.SOURCE_NAME,
            date_from=kwargs['from'],
            date_to=kwargs['to'],
            timestamp=today_date.timestamp(),
        )
        file_full_path = os.path.join(kwargs['source_int_path'], filename)
        os.makedirs(os.path.dirname(file_full_path), exist_ok=True)
        with open(file_full_path, 'w') as csvfile:
            logger.debug('Making file {}'.format(file_full_path))
            writer = csv.DictWriter(csvfile, fieldnames=['Sku'])
            writer.writeheader()
            for product in products:
                writer.writerow({'Sku': product})

    def login(self, sessions_requests):
        self.check_login_credentials(self.b2b_username, self.b2b_password)

        r = do_request(self.BASE_URL + self.LOGIN_PATH, sessions_requests, 'GET')

        self.cookies = 'PHPSESSID=' + r.cookies['PHPSESSID']

        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'es-ES,es;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Content-Length': '19',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Cookie': self.cookies,
            'Host': 'portalempresas.sb.cl',
            'Origin': self.BASE_URL,
            'Referer': self.BASE_URL + self.LOGIN_PATH,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }

        data = {
            'cID': 0,
            'rT': 1,
            'tM': 'light'
        }

        logger.debug("Make request to {}".format(self.BASE_URL + self.CAPTCHA_PATH))
        s = do_request(self.BASE_URL + self.CAPTCHA_PATH, sessions_requests, 'POST', payload=data, headers=headers)

        captcha_image_hash = ast.literal_eval(s.html.html)

        results = []
        for captcha_image in captcha_image_hash:
            t = do_request(self.BASE_URL + self.CAPTCHA_PATH + '?cid=0&hash={}'.format(captcha_image), sessions_requests, 'GET')
            results.append(t.content)

        hash_result = ''
        for hash_, content in zip(captcha_image_hash, results):
            if results.count(content) == 1:
                hash_result = hash_

        if not hash_result:
            logger.error("Error resolving captcha")

        del(data['tM'])
        data['rT'] = 2
        data['pC'] = hash_result
        headers['Content-Length'] = '62'
        logger.debug("Make request to {}".format(self.BASE_URL + self.CAPTCHA_PATH))
        s = do_request(self.BASE_URL + self.CAPTCHA_PATH, sessions_requests, 'POST', payload=data, headers=headers)

        if int(s.content) != 1:
            logger.error("Error resolving captcha, hash is not correct")

        login_form = {
            'idUsuario': self.b2b_username,
            'password': self.b2b_password,
            'captcha-hf': hash_result,
            'captcha-idhf': 0
        }
        headers['Content-Length'] = '113'

        r = do_request(self.BASE_URL + self.VERIFY_LOGIN_PATH, sessions_requests, 'POST', payload=login_form, headers=headers)

        if 'true' in str(r.content):
            return r

        logger.error("Login error")
        SaveErrorLogWhenBadCredentials(r.content, os.environ['SOURCE_INT_PATH'], self.SOURCE_NAME)
        arg = {
            'username': self.b2b_username,
            'portal': self.SOURCE_NAME
        }
        raise ConnectorB2BLoginErrorConnection(arg)

    def logout(self, sessions_requests):
        logout_url = self.BASE_URL + self.LOGOUT_PATH
        do_request(logout_url, sessions_requests, 'GET')

    def generate_files(self, **kwargs):
        if not self.SOURCE_NAME:
            logger.error("SOURCE_NAME not defined in class")
            raise ValueError

        try:
            sessions_requests = HTMLSession()
            self.login(sessions_requests)

            index_url = self.BASE_URL + self.INDEX_PATH
            redirection_url = self.BASE_URL + self.REDIRECTION_PATH

            header_base = {
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'es-ES,es;q=0.9,en-US;q=0.8,en;q=0.7',
                'Connection': 'keep-alive',
                'Cookie': self.cookies,
                'Host': 'portalempresas.sb.cl',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
            }

            header = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
                'Referer': self.BASE_URL + self.LOGIN_PATH,
                'Upgrade-Insecure-Requests': '1',
            }

            header = {**header, **header_base}

            do_request(index_url, sessions_requests, 'GET', headers=header)
            r = do_request(redirection_url, sessions_requests, 'GET', headers=header)

            soup = BeautifulSoup(r.content, 'html.parser')
            buttons = soup.findAll('button')
            empresa = None
            tipoProductos = None
            for button in buttons:
                if self.SOURCE_NAME in button.text:
                    empresa, tipoProductos = button.attrs['onclick'].replace('mantenedor(', '').replace(')', '').split(',')

            if not empresa:
                logger.error("Source not found")
                return []

            valida_url = self.BASE_URL + self.VALIDAMANTENEDOR_PATH
            valida_form = {
                'empresa': empresa,
                'tipoProductos': tipoProductos,
            }
            valida_header = {
                'Accept': '*/*',
                'Content-Length': '25',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'Origin': self.BASE_URL,
                'Referer': self.BASE_URL + self.SOURCE_URL_PATH,
                'X-Requested-With': 'XMLHttpRequest',
            }

            valida_header = {**valida_header, **header_base}

            r = do_request(valida_url, sessions_requests, 'POST', headers=valida_header, payload=valida_form)
            if 'true' not in str(r.content):
                logger.error('Source cannot be validated')
                return []

            mantenedor_url = self.BASE_URL + self.MANTENEDOR_PATH
            do_request(mantenedor_url, sessions_requests, 'GET')

            autentificador_url = self.BASE_URL + self.CONTROLADORES_PATH + '/seguridad/autenticacion.php'
            valida_header['Content-Length'] = '9'
            valida_header['Referer'] = mantenedor_url
            do_request(autentificador_url, sessions_requests, 'POST', headers=valida_header, payload={'ajax': 'true'})

            header_navegador = {
                'Accept': '*/*',
                'Content-Length': '0',
                'Origin': self.BASE_URL,
                'Referer': self.BASE_URL + '/b2b/web/mantenedor.html',
                'X-Requested-With': 'XMLHttpRequest'
            }

            header_navegador = {**header_navegador, **header_base}

            cargarlaboratorio_url = self.BASE_URL + self.CONTROLADORES_PATH + '/cargas/ventas/cargarlaboratorio.php'
            r = do_request(cargarlaboratorio_url, sessions_requests, 'POST', headers=header_navegador)
            soup = BeautifulSoup(r.content, 'html.parser')

            labs = [x.attrs['value'] for x in soup.findAll('option')]
            labs.remove('0')
            self.create_lab_file(labs, **kwargs)

            products = dict()
            for lab in labs:
                header_navegador['Content-Length'] = '7'
                header_navegador['Content-Type'] = 'application/x-www-form-urlencoded; charset=UTF-8'
                consultamaestroproductos_url = self.BASE_URL + self.CONTROLADORES_PATH + '/cargas/miscelaneos/consultamaestroproductos.php'
                r = do_request(consultamaestroproductos_url, sessions_requests, 'POST', headers=header_navegador, payload={'lab': lab})
                soup = BeautifulSoup(r.content, 'html.parser')
                for row in soup.findAll('tr'):
                    sku, desc, cat, barra = row.findAll('td')
                    products[sku.text] = {'product_name': desc.text, 'category_id': cat.text, 'cod_barra': barra.text}
                header_navegador['Content-Length'] = '16'
                consultaproductos_url = self.BASE_URL + self.CONTROLADORES_PATH + '/cargas/miscelaneos/consultaproductos.php'
                r = do_request(consultaproductos_url, sessions_requests, 'POST', headers=header_navegador, payload={'carga': '0', 'lab': lab})
                soup = BeautifulSoup(r.content, 'html.parser')
                for row in soup.findAll('tr'):
                    sku, desc, cat, but = row.findAll('td')
                    products[sku.text]['category_name'] = cat.text
            self.create_product_file(products, **kwargs)

            yesterday = datetime.datetime.strptime(kwargs['from'], '%Y-%m-%d')
            y_string = yesterday.strftime('%d/%m/%Y')

            sales_form = {
                'idFecha': y_string,
                'idLocal': '0',
                'idProductos': '0',
            }

            informe_url = self.BASE_URL + self.CONTROLADORES_PATH + '/ventas/descargadirecta/ventadiaextendido.php'
            today_date = datetime.datetime.today()
            for lab in labs:
                filename = self.FILE_NAME_PATH.format(
                    portal=self.SOURCE_NAME,
                    year=today_date.strftime('%Y'),
                    month=today_date.strftime('%m'),
                    filetype='ventas',
                    client=kwargs['b2b_username'],
                    empresa=lab,
                    date_from=kwargs['from'],
                    date_to=kwargs['to'],
                    timestamp=today_date.timestamp(),
                )
                filename = os.path.join(kwargs['source_int_path'], filename)
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                sales_form['idProveedor'] = lab
                r = do_request(informe_url, sessions_requests, 'POST', payload=sales_form)
                with open(filename, 'wb') as fl:
                    logger.debug('Making file {}'.format(filename))
                    fl.write(r.content)

            stock_url = self.BASE_URL + self.CONTROLADORES_PATH + '/inventario/descargadirecta/stockvssugeridoextendido.php'
            del(sales_form['idFecha'])
            for lab in labs:
                filename = self.FILE_NAME_PATH.format(
                    portal=self.SOURCE_NAME,
                    year=today_date.strftime('%Y'),
                    month=today_date.strftime('%m'),
                    filetype='stock',
                    client=kwargs['b2b_username'],
                    empresa=lab,
                    date_from=kwargs['from'],
                    date_to=kwargs['to'],
                    timestamp=today_date.timestamp(),
                )
                filename = os.path.join(kwargs['source_int_path'], filename)
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                sales_form['idProveedor'] = lab
                r = do_request(stock_url, sessions_requests, 'POST', payload=sales_form)
                table_data = [[cell.text for cell in row("td")] for row in BeautifulSoup(r.content, 'html.parser')("tr")]
                if table_data:
                    table_data[0][1] = 'Descripción Producto'
                    with open(filename, 'w') as fl:
                        logger.debug('Making file {}'.format(filename))
                        writer = csv.writer(fl)
                        for row in table_data:
                            writer.writerow(row)
                else:
                    logger.debug("No stock data available in portal")

            quiebre_url = self.BASE_URL + self.CONTROLADORES_PATH + '/inventario/buscainventario.php'
            quiebre_form = {
                'idProductos': 0,
                'idLocal': 0,
            }
            ignore_product = list()
            for lab in labs:
                quiebre_form['idProveedor'] = lab
                r = do_request(quiebre_url, sessions_requests, 'POST', payload=quiebre_form)
                table = BeautifulSoup(r.content, 'html.parser').find('table', class_='table')
                if table:
                    header = [i.text for i in table.find('tr', class_='sticky-row').findAll('td')]
                    for row in table.findAll('tr', class_=None):
                        prod = dict()
                        for key, value in zip(header, row.findAll('td')):
                            prod[key] = value.text
                        if prod['Motivo Quiebre']:
                            ignore_product.append(prod['Sku'])

            self.create_quiebre_file(ignore_product, **kwargs)

            self.logout(sessions_requests)

        except ConnectionError:
            logger.debug("Connection Error, wait 10 second to repeat requests.")
            time.sleep(10)
            return self.generate_files(**kwargs)

        # when another person enters with the credentials,
        # the remote server closes its connection raising this exception
        except ProtocolError:
            raise ConnectorB2BClientInPortalError

        return [{'from': kwargs['from'], 'to': kwargs['to'], 'status': 'ok'}, ]


class EmpresasSBB2BBaseConnector(B2BConnector):

    file_pattern = '{filetype}_{b2b_username}_{b2b_empresa}_*.csv'

    SOURCE_NAME = ''

    fixed_sub_root_folder = 'b2b-files'
    fixed_sub_sales_folder = 'ventas'

    @classmethod
    def get_instance(cls, **kwargs):
        query_date = kwargs['date_start']
        repository_path = kwargs['repository_path']
        b2b_username = kwargs['b2b_username']
        return cls(query_date, b2b_username, repository_path)

    def __init__(self, query_date, b2b_username, base_path):
        # FIXME change this horrible thing
        self.date_start = query_date.strftime("%Y-%m-%d 00:00:00 00:00")
        self._date = query_date
        self.user_name = b2b_username
        self.repository_path = os.path.join(
            base_path, self.fixed_sub_root_folder, self.SOURCE_NAME, self.fixed_sub_sales_folder)

    def identify_file(self, file_name, mode='timestamp'):
        file_path = os.path.join(self.repository_path, '**', file_name)
        files = glob.glob(file_path, recursive=True)
        file_name = None
        ts_file = 0

        if mode == 'timestamp':
            initial_timestamp = datetime.datetime.timestamp(datetime.datetime.combine(self._date, datetime.time(0, 0)))
            final_timestamp = datetime.datetime.timestamp(datetime.datetime.combine(self._date, datetime.time(23, 59)))
            for filename in files:
                timestamp_for_file = float(filename.split('_')[-1].split('.csv')[0])
                if initial_timestamp <= timestamp_for_file and timestamp_for_file <= final_timestamp:
                    logger.debug("File whit data {}".format(filename))
                    if ts_file < timestamp_for_file:
                        file_name = filename
                        ts_file = timestamp_for_file

        if mode == 'date':
            for filename in files:
                timestamp_for_file = float(filename.split('_')[-1].split('.csv')[0])
                date_from = datetime.datetime.strptime(filename.split('_')[3], '%Y-%m-%d').date()
                date_to = datetime.datetime.strptime(filename.split('_')[4], '%Y-%m-%d').date()
                if date_from <= self._date and self._date <= date_to:
                    logger.debug("File whit data {}".format(filename))
                    if ts_file < timestamp_for_file:
                        file_name = filename
                        ts_file = timestamp_for_file

        if not file_name:
            logger.debug("No file to process!")
        else:
            logger.debug("File to process {}".format(file_name))

        return file_name


class EmpresasSBB2BSalesConnector(EmpresasSBB2BBaseConnector):

    def stock_row_in_records(self, row, records):
        for record in records:
            if row['Sku'] == record['Sku']:
                return True
        return False

    def detalle_venta(self):
        if not self.SOURCE_NAME:
            logger.error("No SOURCE_NAME defined")
            return []

        filetypes_lists = ['labs', 'quiebre']
        filetypes_search_method = ['date', 'timestamp']
        lists = dict()
        products = dict()

        for filetype, method in zip(filetypes_lists, filetypes_search_method):
            file_name = self.file_pattern.format(filetype=filetype, b2b_username=self.user_name, b2b_empresa=self.SOURCE_NAME)
            file_name = self.identify_file(file_name, method)

            if not file_name:
                logger.debug("Can't obtain {} data".format(filetype))
                lists[filetype] = []
                continue

            with open(file_name, 'r') as csvfile:
                reader = csv.reader(csvfile)
                next(reader)
                lists[filetype] = [row[0] for row in reader]

        if not lists['labs']:
            return []

        file_name = self.file_pattern.format(filetype='products', b2b_username=self.user_name, b2b_empresa=self.SOURCE_NAME)
        file_name = self.identify_file(file_name, 'date')
        if not file_name:
            logger.debug("Can't obtain product data")
            return []
        with open(file_name, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                products[row['Sku']] = dict(row)

        sales_records = []
        for lab in lists['labs']:
            file_name = self.file_pattern.format(filetype='ventas', b2b_username=self.user_name, b2b_empresa=lab)
            file_name = self.identify_file(file_name, 'date')
            if file_name:
                with open(file_name, 'r') as csvfile:
                    reader = csv.DictReader(csvfile, delimiter=';')
                    for row in reader:
                        row_date = datetime.datetime.strptime(row['Fecha'], "%d/%m/%Y").strftime("%Y-%m-%d")
                        if row_date == self._date.strftime("%Y-%m-%d"):
                            sales_records.append(dict(row))
            else:
                logger.debug("No sales file to process for lab {}".format(lab))

        if not sales_records:
            logger.debug("No sales data for this day")

        stock_records = []
        for lab in lists['labs']:
            file_name = self.file_pattern.format(filetype='stock', b2b_username=self.user_name, b2b_empresa=lab)
            file_name = self.identify_file(file_name, 'date')
            if file_name:
                with open(file_name, 'r') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        stock_records.append(dict(row))
            else:
                logger.debug("No stock file to process for lab {}".format(lab))

        records = []

        for row in sales_records:
            new_row = dict()
            datetime_row = self._date.strftime("%Y-%m-%d 00:00:00 00:00")
            new_row = {k: v for k, v in row.items()}
            new_row['Datetime'] = datetime_row
            base_sale = {
                'Sku': row['SKU'],
                'Descripción Producto': products[new_row['SKU']]['product_name'],
                'Id Sucursal': row['N Local'],
                'Descripción': row['Descripcion Local'],
            }
            records.append({**new_row, **products[new_row['SKU']], **base_sale})

        if stock_records:
            dict_zero_values = {'Venta': 0, 'Unidades': 0, }
            for row in stock_records:
                if row['Sku'] in lists['quiebre']:
                    continue
                if not self.stock_row_in_records(row, records):
                    new_row = dict()
                    datetime_row = self._date.strftime("%Y-%m-%d 00:00:00 00:00")
                    new_row = {k: v for k, v in row.items()}
                    new_row['Datetime'] = datetime_row
                    records.append({**new_row, **products[new_row['Sku']], **dict_zero_values})
        else:
            logger.debug("No stock data from today, generating files from sales only")

        return records


class EmpresasSBB2BStockConnector(EmpresasSBB2BBaseConnector):

    def detalle_venta(self):
        if not self.SOURCE_NAME:
            logger.error("No SOURCE_NAME defined")
            return []

        filetypes_lists = ['labs']
        lists = dict()
        products = dict()

        for filetype in filetypes_lists:
            file_name = self.file_pattern.format(filetype=filetype, b2b_username=self.user_name, b2b_empresa=self.SOURCE_NAME)
            file_name = self.identify_file(file_name, 'timestamp')

            if not file_name:
                logger.debug("Can't obtain {} data".format(filetype))
                return []

            with open(file_name, 'r') as csvfile:
                reader = csv.reader(csvfile)
                next(reader)
                lists[filetype] = [row[0] for row in reader]

        file_name = self.file_pattern.format(filetype='products', b2b_username=self.user_name, b2b_empresa=self.SOURCE_NAME)
        file_name = self.identify_file(file_name, 'timestamp')
        if not file_name:
            logger.debug("Can't obtain product data")
            return []
        with open(file_name, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                products[row['Sku']] = dict(row)

        stock_records = []
        for lab in lists['labs']:
            file_name = self.file_pattern.format(filetype='stock', b2b_username=self.user_name, b2b_empresa=lab)
            file_name = self.identify_file(file_name, 'timestamp')
            if file_name:
                with open(file_name, 'r') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        stock_records.append(dict(row))
            else:
                logger.debug("No stock data for lab {}".format(lab))

        if stock_records:
            records = []
            date_time = self._date.strftime("%Y-%m-%d 00:00:00 00:00")
            base = {
                'Datetime': date_time
            }
            for row in stock_records:
                records.append({**row, **products[row['Sku']], **base})
            return records
        else:
            logger.debug("No stock data for today!")
            return stock_records

class PageRangeDTO:

    def __init__(self, untilPage=1, sincePage=1):
        self.untilPage = untilPage
        self.sincePage = sincePage


class OrderCriteriaDTO:

    def __init__(self, propertyname='ORDERNUMBER', ascending=False):
        self.propertyname = propertyname
        self.ascending = ascending


class OrderReportInitParamDTO:

    def __init__(
            self,
            salestoreid=None,
            pageNumber=1,
            locationid=-1,
            sendnumber=None,
            orderby=[
                OrderCriteriaDTO()],
            ocnumber=-1,
            vendorid=None,
            until=None,
            orderstatetypeid=9,
            rows=100,
            filtertype=None,
            since=None):
        self.salestoreid = salestoreid
        self.pageNumber = pageNumber
        self.locationid = locationid
        self.sendnumber = sendnumber
        self.orderby = orderby
        self.ocnumber = ocnumber
        self.vendorid = vendorid
        self.until = until
        self.orderstatetypeid = orderstatetypeid
        self.rows = rows
        self.filtertype = filtertype
        self.since = since

    def set_filtertype(self, stateid):
        self.filtertype = stateid


class SalesInventory:

    def __init__(self,
                 levelProduct=None,
                 groupByProduct=None,
                 showInventory=None,
                 levelLocal=None,
                 excludeProductsWithoutSales=None,
                 productOrCategory=None,
                 catLocRetailer=None,
                 typeOfMark=None,
                 season=None,
                 levelToGroupProduct=None,
                 keyLocal=None,
                 formatType=None,
                 viewProductOrLocal=None,
                 levelToGroupLocal=None,
                 typeOfLocal=None,
                 lowLimitDateSales=None,
                 activeProducts=None,
                 keyProduct=None,
                 levelToGroup=None,
                 pvkey=None,
                 groupByLocal=None,
                 highLimitDateSales=None,
                 activeLocals=None,
                 catProdRetailer=None,
                 excludeProductsWithoutInventory=None,
                 localOrCategory=None

                 ):

        self.levelProduct = levelProduct
        self.groupByProduct = groupByProduct
        self.showInventory = showInventory
        self.levelLocal = levelLocal
        self.excludeProductsWithoutSales = excludeProductsWithoutSales
        self.productOrCategory = productOrCategory
        self.typeOfMark = typeOfMark
        self.catLocRetailer = catLocRetailer
        self.levelToGroupProduct = levelToGroupProduct
        self.keyLocal = keyLocal
        self.formatType = formatType
        self.viewProductOrLocal = viewProductOrLocal
        self.levelToGroupLocal = levelToGroupLocal
        self.typeOfLocal = typeOfLocal
        self.lowLimitDateSales = lowLimitDateSales
        self.activeProducts = activeProducts
        self.keyProduct = keyProduct
        self.pvkey = pvkey
        self.levelToGroup = levelToGroup
        self.highLimitDateSales = highLimitDateSales
        self.groupByLocal = groupByLocal
        self.season = season
        self.activeLocals = activeLocals
        self.catProdRetailer = catProdRetailer
        self.excludeProductsWithoutInventory = excludeProductsWithoutInventory
        self.localOrCategory = localOrCategory

    def set_pvkey(self, pvkey):
        self.pvkey = pvkey

    def set_LimitDateSales(self, date):
        self.lowLimitDateSales = date
        self.highLimitDateSales = date
