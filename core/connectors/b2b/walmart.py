import csv
import datetime
import glob
import os
import time

import pandas as pd

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from pyvirtualdisplay import Display
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager

from core.connectors.b2b.base import B2BConnector
from core.connectors.b2b.utils import ConnectorB2BLoginErrorConnection
from core.connectors.b2b.utils import WalmartReportWaitingTimeExceded
from utils.logger import logger


class WalmartB2BConnector(B2BConnector):
    API_LOGIN_PATH = 'https://retaillink.login.wal-mart.com/api/login'

    BASE_URL = 'https://retaillink.wal-mart.com'
    HOME_PATH = '/rl_portal/'
    REPORT_PATH = '#/request-page'

    SOLICITUDES_PATH = '/Decision_support/status_get_data.aspx?ApplicationId=300&bPortlet=False'
    REQUEST_PATH = '/Decision_support/Submit_Request.aspx?submitnow=false&applicationid=300'
    BORRAR_PATH = '/rl_portal_services/api/Site/DeleteRequestStatus?JobId={}'
    DOWNLOAD_PATH = '/rl_home_services/home/DownloadReport?jobid={}'

    LOGOUT_PATH = '/logout'

    FILE_NAME_PATH = 'b2b-files/{portal}/ventas/{year}/{month}/{filetype}_{client}_{empresa}_{date_from}_{date_to}_{timestamp}.csv'

    @classmethod
    def get_instance(cls, **kwargs):
        b2b_username = kwargs['b2b_username']
        b2b_password = kwargs['b2b_password']
        return cls(b2b_username, b2b_password)

    def __init__(self, b2b_username, b2b_password):
        self.b2b_username = b2b_username
        self.b2b_password = b2b_password

    def check_if_csv(self, path, delimiter=None, encoding='utf-8'):
        try:
            data = pd.read_csv(path, encoding=encoding, sep=delimiter, header=None)
            if len(list(data.columns)) <= 1:
                logger.error("No csv file")
                return False
        except Exception as e:
            logger.error(e)
            return False
        return True

    def login(self, driver):
        self.check_login_credentials(self.b2b_username, self.b2b_password)

        driver.get(self.BASE_URL)
        delay = 3  # seconds
        try:
            username = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, "//input[@type='text']")))
            password = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, "//input[@type='password']")))
            button_login = WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, "//button[@data-automation-id='loginBtn']")))
            username.send_keys(self.b2b_username)
            password.send_keys(self.b2b_password)
            button_login.click()
        except TimeoutException:
            logger.debug("Selenium login Time out Error")
        except NoSuchElementException:
            logger.debug("elements to send keys not found")

        time.sleep(15)
        if len(driver.find_elements_by_class_name('alert__message___wvlXn')) > 0:
            arg = {
                'username': self.b2b_username,
                'portal': 'Walmart'
            }
            raise ConnectorB2BLoginErrorConnection(arg)

    def execute_script(self, driver, id_elem, text):
        elem = driver.find_element_by_id(id_elem)
        driver.execute_script('''
            var elem = arguments[0];
            var value = arguments[1];
            elem.value = value;
        ''', elem, text)

    def solicitud_venta(self, driver, report_name, initial_date, final_date):
        url_solicitud = self.BASE_URL + self.REQUEST_PATH
        wait = WebDriverWait(driver, 60)

        initial_date = datetime.datetime.strptime(
            initial_date, '%Y-%m-%d').strftime('%m-%d-%Y')
        final_date = datetime.datetime.strptime(
            final_date, '%Y-%m-%d').strftime('%m-%d-%Y')

        driver.find_element_by_tag_name('body').send_keys(Keys.COMMAND + 't')
        driver.get(url_solicitud)
        wait.until(EC.presence_of_element_located((By.ID, "Description")))

        criteria = '4	:	E200001	:	F2000010	:	report	:	column	:	Report Columns 	:	Núm del Artículo Principal	~	1799	|	ID de Marca	~	9916	|	Descripción de Marca	~	3359	|	Número de Artículo de Sistema Legado	~	24315	|	Desc  Artículo Principal	~	1799a	|	Descripción Categoría del Dpto	~	354	|	Descripción Subcategoría del Dpto	~	353	|	Núm de Tienda	~	2	|	Nombre de Tienda	~	1201	|	Venta POS	~	2605	|	Cnt POS	~	2610	|	Costo POS	~	2618	|	Diario (Sólo POS)	~	3b	|	Cantidad Actual en Existentes de la tienda	~	2730	|	Costo Actual en Existentes de la tienda	~	5217	^	1	:	E210035	:	F1010125	:	what	:	Filter	:	 Nº Artículo   No Es Uno De  0 And	~	-AND-	~	1,11	~	0	^	1	:	E100001	:	F1000010	:	when	:	Filter	:	 Fecha POS (mm-dd-aaaa)   Rango 1 Está Entre  {initial} and {final} And	~	-AND-	~	204,8	~	{initial}	~	{final}	^	2	:	E10102	:	F101020	:	where	:	wm_store_type	:	-1	~	Selections Include	:	Tipos de Tienda	$	Todas las Tiendas	~	DSS1	~	1	~	1	~	4	~	0'.format(
            initial=initial_date,
            final=final_date)

        self.execute_script(driver, 'Description', report_name)
        self.execute_script(driver, 'Compressed', '0')
        self.execute_script(driver, 'submitnow', 'true')
        self.execute_script(driver, 'flag', '1')
        self.execute_script(driver, 'Format', '4')
        self.execute_script(driver, 'QId', 'Q1362')
        self.execute_script(driver, 'RequestId', '37003285')
        self.execute_script(driver, 'Criteria', criteria)
        self.execute_script(driver, 'AppId', '300')
        self.execute_script(driver, 'ExeId', '1362')
        self.execute_script(driver, 'Environment', '')
        self.execute_script(driver, 'callOnload', 'true')
        self.execute_script(driver, 'DivisionId', '1')
        self.execute_script(driver, 'CountryCode', 'K2')
        self.execute_script(driver, 'hdnAllowSubmit', 'True')

        driver.find_element_by_id('SubmitRequest').submit()
        time.sleep(3)
        driver.find_element_by_tag_name('body').send_keys(Keys.COMMAND + 'w')

    def save_file(self, driver, date_from, date_to, id_report, **kwargs):
        today_date = datetime.datetime.today()
        file_name = self.FILE_NAME_PATH.format(
            portal='Walmart',
            filetype='ventas',
            client=kwargs['b2b_username'],
            empresa='walmart',
            year=today_date.strftime('%Y'),
            month=today_date.strftime('%m'),
            date_from=date_from,
            date_to=date_to,
            timestamp=today_date.timestamp(),
        )
        url_download = self.BASE_URL + self.DOWNLOAD_PATH.format(id_report)
        logger.debug(f"Downloading file from {url_download}")
        driver.get(url_download)
        time.sleep(10)
        download_folder = os.path.join(kwargs['source_int_path'], 'b2b-files/Walmart/raw')
        file_full_path = os.path.join(kwargs['source_int_path'], file_name)
        file_exist = False
        for file in glob.glob(os.path.join(download_folder, f'*_{id_report}*.txt')):
            logger.debug("Saving file {}".format(file_full_path))
            os.makedirs(os.path.dirname(file_full_path), exist_ok=True)
            os.rename(file, file_full_path)
            file_exist = True

        if not file_exist:
            logger.debug("There was an error downloading the report, it needs to be generated again.")
            return False

        if self.check_if_csv(file_full_path, delimiter='\t', encoding='latin1'):
            return True
        else:
            os.remove(file_full_path)
            logger.debug(f"It was an error creating file {file_full_path}, trying again..")
            return False

    def check_status(self, driver):
        logger.debug("Check report status")
        url_home = self.BASE_URL + self.HOME_PATH + self.REPORT_PATH
        if url_home in driver.current_url:
            driver.get(self.BASE_URL + self.HOME_PATH)
        driver.get(url_home)
        wait = WebDriverWait(driver, 120)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'page-title')))
        time.sleep(10)
        elements = driver.find_elements_by_class_name('rt-tr-group')
        status_list = list()
        for ele in elements:
            p_elems = ele.find_elements_by_tag_name('p')
            if p_elems:
                name = p_elems[0].get_attribute('data-tip').split(' , ')[0]
                status = p_elems[2].text
                logger.debug(f"Found {name} status {status}")
                status_list.append({
                    'Name': name,
                    'ID': p_elems[0].get_attribute('data-tip').split(': ')[-1],
                    'Status': status,
                    'url': self.BASE_URL + self.DOWNLOAD_PATH.format(p_elems[0].get_attribute('data-tip').split(': ')[-1]) if len(ele.find_elements_by_class_name('icon-download')) > 0 else None
                })
        return status_list

    def borrar_reporte(self, id_report, driver):
        logger.debug(f"Deleting report {id_report}")
        url_home = self.BASE_URL + self.BORRAR_PATH.format(id_report)
        driver.get(url_home)
        time.sleep(5)

    def logout(self, driver):
        url = self.BASE_URL + self.LOGOUT_PATH
        driver.get(url)
        time.sleep(3)

    def generate_files(self, **kwargs):
        download_folder = os.path.join(kwargs['source_int_path'], 'b2b-files/Walmart/raw')
        periods_checked = list()

        with Display(visible=0):
            options = webdriver.ChromeOptions()
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_experimental_option("prefs", {
                "download.default_directory": download_folder,
                "download.prompt_from_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": False
            })

            driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(),
                                      service_args=['--verbose', '--log-path=/tmp/chromedriver.log'],
                                      chrome_options=options)

            self.login(driver)

            period_days = list()
            d_from = datetime.datetime.strptime(kwargs['from'], '%Y-%m-%d')
            d_to = datetime.datetime.strptime(kwargs['to'], '%Y-%m-%d')
            while d_from <= d_to:
                period_days.append(d_from)
                d_from += datetime.timedelta(days=1)

            status_list = self.check_status(driver)
            for ele in status_list:
                if 'Report ' in ele['Name'] and ele['url']:
                    decompose_name = ele['Name'].split(' ')
                    if len(decompose_name) == 4:
                        date_from = datetime.datetime.strptime(decompose_name[1], '%Y-%m-%d')
                        date_to = datetime.datetime.strptime(decompose_name[3], '%Y-%m-%d')
                        if self.save_file(driver, decompose_name[1], decompose_name[3], ele['ID'], **kwargs):
                            self.borrar_reporte(ele['ID'], driver)
                            periods_checked.append({
                                'from': decompose_name[1],
                                'to': decompose_name[3],
                                'status': 'ok',
                            })
                            if date_from == date_to and date_from in period_days:
                                period_days.remove(date_from)
                            while date_from <= date_to:
                                if date_from in period_days:
                                    period_days.remove(date_from)
                                date_from += datetime.timedelta(days=1)
                        else:
                            self.borrar_reporte(ele['ID'], driver)

            if not period_days:
                self.logout(driver)
                return periods_checked

            oldest_date = min(period_days).strftime('%Y-%m-%d')
            newest_date = max(period_days).strftime('%Y-%m-%d')
            report_name = f'Report {oldest_date} - {newest_date}'

            time_wait_sec = 8
            downloaded_document = False

            tries = 1
            bad_file_detected = 0
            while not downloaded_document and tries < 15:
                report_info = dict()

                status_list = self.check_status(driver)

                if any(report_name in status['Name'] for status in status_list):
                    for ele in status_list:
                        if report_name in ele['Name']:
                            report_info = ele
                            continue
                    logger.debug("Document in state {}".format(report_info['Status']))
                    if 'Error' in report_info['Status']:
                        self.borrar_reporte(report_info['ID'], driver)
                    elif report_info['url']:
                        if self.save_file(driver, oldest_date, newest_date, report_info['ID'], **kwargs):
                            self.borrar_reporte(report_info['ID'], driver)
                            downloaded_document = True
                            periods_checked.append({
                                'from': oldest_date,
                                'to': newest_date,
                                'status': 'ok',
                            })
                            bad_file_detected = 0
                        else:
                            self.borrar_reporte(report_info['ID'], driver)
                            bad_file_detected += 1
                            time_wait_sec = 8
                            if bad_file_detected > 3:
                                logger.debug("Could not download file for period {} to {}".format(oldest_date, newest_date))
                                downloaded_document = True
                    else:
                        logger.debug(
                            "Wait {} seconds to continue".format(time_wait_sec))
                        time.sleep(time_wait_sec)
                else:
                    logger.debug("Requesting report {}".format(report_name))
                    self.solicitud_venta(driver, report_name, oldest_date, newest_date)
                    logger.debug(f"Wait {time_wait_sec} seconds to continue")
                    time.sleep(time_wait_sec)
                if time_wait_sec < 128:
                    time_wait_sec *= 2
                tries += 1

            if tries >= 15:
                self.logout(driver)
                raise WalmartReportWaitingTimeExceded

            self.logout(driver)

        return periods_checked


class WalmartB2BFileBaseConnector(B2BConnector):

    file_pattern = 'ventas_{b2b_username}_{b2b_empresa}_*.csv'
    fixed_sub_folder = 'Walmart'
    fixed_sub_root_folder = 'b2b-files'
    fixed_sub_sales_folder = 'ventas'

    header_csv = [
        'Núm del Artículo Principal',
        'ID de Marca',
        'Descripción de Marca',
        'Número de Artículo de Sistema Legado',
        'Desc  Artículo Principal',
        'Descripción Categoría del Dpto',
        'Descripción Subcategoría del Dpto',
        'Núm de Tienda',
        'Nombre de Tienda',
        'Venta POS',
        'Cnt POS',
        'Costo POS',
        'Diario (Sólo POS)',
        'Cantidad Actual en Existentes de la tienda',
        'Costo Actual en Existentes de la tienda',
    ]

    def parse_metrics(self, new_row):
        new_row['Venta POS'] = float(new_row['Venta POS'])
        new_row['Cnt POS'] = float(new_row['Cnt POS'])
        new_row['Costo POS'] = float(new_row['Costo POS'])
        new_row['Cantidad Actual en Existentes de la tienda'] = float(new_row['Cantidad Actual en Existentes de la tienda'])
        new_row['Costo Actual en Existentes de la tienda'] = float(new_row['Costo Actual en Existentes de la tienda'])

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
            base_path, self.fixed_sub_root_folder, self.fixed_sub_folder, self.fixed_sub_sales_folder)


class WalmartB2BFileConnector(WalmartB2BFileBaseConnector):

    def detalle_venta(self):
        file_name = self.file_pattern.format(b2b_username=self.user_name, b2b_empresa='walmart')
        file_path = os.path.join(self.repository_path, '**', file_name)
        files = glob.glob(file_path, recursive=True)
        file_name = None
        ts_file = 0
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
            logger.debug("No files to process!")
            return []
        logger.debug("File to process {}".format(file_name))

        records = []
        date_timestamp = datetime.datetime.fromtimestamp(ts_file).strftime("%Y-%m-%d 00:00:00 00:00")

        if not os.path.exists(file_name):
            logger.debug('File doesn\'t exists {}'.format(file_name))
        else:
            with open(file_name, 'r', encoding='latin1') as csv_file:
                reader = csv.DictReader(csv_file, fieldnames=self.header_csv, delimiter='\t')
                for row in reader:
                    new_row = dict()
                    datetime_row = datetime.datetime.strptime(row['Diario (Sólo POS)'], '%Y/%m/%d').strftime("%Y-%m-%d 00:00:00 00:00") if row['Diario (Sólo POS)'] else self.date_start
                    if datetime_row == self.date_start:
                        new_row = {k: v for k, v in row.items()}
                        new_row['Datetime'] = datetime_row
                        new_row['Timestamp_datetime'] = date_timestamp
                        self.parse_metrics(new_row)
                        records.append(new_row)

        return records


class WalmartB2BFileStockConnector(WalmartB2BFileBaseConnector):

    def detalle_venta(self):
        file_name = self.file_pattern.format(b2b_username=self.user_name, b2b_empresa='walmart')
        file_path = os.path.join(self.repository_path, '**', file_name)
        files = glob.glob(file_path, recursive=True)
        initial_timestamp = datetime.datetime.timestamp(datetime.datetime.combine(self._date, datetime.time(0, 0)))
        final_timestamp = datetime.datetime.timestamp(datetime.datetime.combine(self._date, datetime.time(23, 59)))
        file_name = None
        ts_file = 0
        for filename in files:
            timestamp_for_file = float(filename.split('_')[-1].split('.csv')[0])
            if initial_timestamp <= timestamp_for_file and timestamp_for_file <= final_timestamp:
                logger.debug("File whit data {}".format(filename))
                if ts_file < timestamp_for_file:
                    file_name = filename
                    ts_file = timestamp_for_file

        if not file_name:
            logger.debug("No files to process!")
            return []
        logger.debug("File to process {}".format(file_name))

        records = []
        date_timestamp = datetime.datetime.fromtimestamp(ts_file).strftime("%Y-%m-%d 00:00:00 00:00")

        if not os.path.exists(file_name):
            logger.debug('File doesn\'t exists {}'.format(file_name))
        else:
            with open(file_name, 'r', encoding='latin1') as csv_file:
                reader = csv.DictReader(csv_file, fieldnames=self.header_csv, delimiter='\t')
                product = None
                store = None
                for row in reader:
                    new_row = dict()
                    if not (row['Número de Artículo de Sistema Legado'] == product and row['Núm de Tienda'] == store):
                        new_row = {k: v for k, v in row.items()}
                        new_row['Datetime'] = date_timestamp
                        self.parse_metrics(new_row)
                        records.append(new_row)
                        product = row['Número de Artículo de Sistema Legado']
                        store = row['Núm de Tienda']

        return records
