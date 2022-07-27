import csv
import datetime
import glob
import os
import time
import pathlib

import pandas as pd
from selenium.webdriver.common.keys import Keys
from seleniumwire import webdriver
from pyvirtualdisplay import Display
from selenium.webdriver import ActionChains
from webdriver_manager.chrome import ChromeDriverManager

from core.connectors.b2b.base import B2BConnector, BBReCommercePortalConnector
from core.connectors.b2b.utils import ConnectorB2BGenericError
from utils.logger import logger


class LaPolarB2BConnector(BBReCommercePortalConnector):
    PORTAL = 'LaPolar'
    POST_URL = 'https://b2b.lapolar.cl/BBRe-commerce/main/UIDL/?v-uiId=0'

    UNIQUE_PARAMS = {
        'popup_messages': [
            '/html/body/div[2]/div[6]/div/div/div[2]/div[2]',
            '/html/body/div[2]/div[4]/div/div/div[2]/div[2]',
            '/html/body/div[2]/div[2]/div/div/div[2]/div[2]'
        ],
        'Comercial Menu': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[1]/div/div/div[3]/div/span[3]',
        'Informe de Ventas': '/html/body/div[2]/div[2]/div/div/span[1]',
        'Empresa Selector': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[1]/div/div[2]/div/div/div[3]/div/input',
        'Span Options': '/html/body/div[2]',
        'Calendar From': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[4]/div/div/div[1]/div/div[3]/div/div[1]/div/div[3]/div/button',
        'Calendar To': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[4]/div/div/div[1]/div/div[3]/div/div[3]/div/div[3]/div/button',
        'Estacion Selector': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[1]/div/div[2]/div/div/div[3]/div/input',
        'Search Button': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[5]/div/div/div',
        'Download Click 1': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div/div[2]/div/div/div/div/div[1]/div/div/div[3]/div/div[1]/div',
        'Download Click 2': '/html/body/div[2]/div[2]/div/div/div[2]/div',
        'Download Click 3': '/html/body/div[2]/div[3]/div/div/div[3]/div/div/div[3]/div/div[1]/div',
        'Close Download Popup': '/html/body/div[2]/div[3]/div/div/div[2]/div[2]',
        'Logout Click 1': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[1]/div/div/div[5]/div/span',
        'Logout Click 2': '/html/body/div[2]/div[2]/div/div/span'
    }

    def login_form(self, driver):
        driver.get("https://b2b.lapolar.cl/")
        time.sleep(4)


class LaPolarB2BFileBaseConnector(B2BConnector):

    file_pattern = '{filetype}_{b2b_username}_{b2b_empresa}_*.csv'
    fixed_sub_folder = 'LaPolar'
    fixed_sub_root_folder = 'b2b-files'
    fixed_sub_sales_folder = 'ventas'

    ENCODE = 'latin1'

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
            if last_file or (date_from <= self._date and self._date <= date_to):
                logger.debug("File whit data {}".format(filename))
                if ts_file < timestamp_for_file:
                    file_name = filename
                    ts_file = timestamp_for_file
        if file_name:
            logger.debug("File to process {}".format(file_name))
            return (file_name, ts_file)
        else:
            logger.debug("No files to process!")
            return (None, None)

    def detalle_venta(self):
        data = []
        file_name, ts_file = self.get_file_name('ventas')
        file_name_excel, ts_file_excel = self.get_file_name('products', last_file=True)

        if not file_name:
            return data

        data_product = dict()
        if file_name_excel:
            with open(file_name_excel, 'r', encoding=self.ENCODE) as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    new_row = row.copy()
                    if new_row['PLU'] not in data_product:
                        data_product[new_row['PLU']] = new_row

        empty_data_product_dict = {
            'Marca': 'Sin Marca',
            'Temporada': 'Sin Temporada',
            'Estado': 'Sin Estado',
        }

        with open(file_name, 'r', encoding=self.ENCODE) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                new_row = row
                new_row['Datetime'] = self.date_start
                if new_row['PLU'] in data_product:
                    final_row = {**new_row, **data_product[new_row['PLU']]}
                else:
                    final_row = {**new_row, **empty_data_product_dict}
                self.parse_metrics(final_row)
                data.append(final_row)
        return self.complement_data(data)

    def get_file_name(self, filetype, last_file=False):
        raise NotImplementedError

    def parse_metrics(self, row):
        row['VENTA_PERIODO(U)'] = float(row['VENTA_PERIODO(U)'])
        row['VENTA_PERIODO(MV)'] = float(row['VENTA_PERIODO(MV)'])
        row['VENTA_PERIODO(MC)'] = float(row['VENTA_PERIODO(MC)'])
        if 'INV_DISP(U)' in row:
            row['INV_DISP(U)'] = float(row['INV_DISP(U)'])
            row['INV_DISPONIBLE($)'] = float(row['INV_DISPONIBLE($)'])

    def complement_data(self, data):
        return data


class LaPolarB2BFileConnector(LaPolarB2BFileBaseConnector):

    def get_file_name(self, filetype, last_file=False):
        return self.find_file(filetype, last_file=last_file)


class LaPolarB2BStockFileConnector(LaPolarB2BFileBaseConnector):

    def get_file_name(self, filetype, last_file=False):
        return self.find_file(filetype, per_timestamp=True, last_file=last_file)


class LaPolarB2BOCConnector(LaPolarB2BConnector):
    FILE_TIME_OF_LIFE = 82800

    def detalle_venta(self):
        download_folder = os.path.join(os.environ['SOURCE_INT_PATH'], f'b2b-files/{self.PORTAL}/raw')

        if not self.UNIQUE_PARAMS:
            logger.error('UNIQUE_PARAMS not defined')
            raise ConnectorB2BGenericError

        #display = Display(visible=not bool(os.environ.get('WIVO_ENV', True)))
        display = Display(visible=0)
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

            for popup_xpath in self.UNIQUE_PARAMS['popup_messages']:
                if driver.find_elements_by_xpath(popup_xpath):
                    driver.find_element_by_xpath(popup_xpath).click()
                    time.sleep(1)

            driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[1]/div/div/div[3]/div/span[4]').click()
            time.sleep(1)
            driver.find_element_by_xpath('/html/body/div[2]/div[2]/div/div/span[2]').click()
            time.sleep(10)

            driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[3]/div/div/div').click()
            time.sleep(10)

            if driver.find_elements_by_xpath('/html/body/div[2]/div[3]/div/div/div[3]/div/div/div[3]/div/div[3]/div'):
                driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/div/div[3]/div/div/div[3]/div/div[3]/div').click()
                time.sleep(2)
            else:
                driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[2]/div/div[1]/div/div/div[3]/div/div[1]/div').click()
                time.sleep(3)
                # driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div[2]/div/div/div/div/span[2]/label').click()
                detalle_excel = driver.find_element_by_xpath('/html/body/div[2]/div[2]/div/div/div[2]/div')
                detalle_excel.click()
                # driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div[3]/div/div/div[1]/div').click()
                time.sleep(3)
                driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div[2]/div/div/div/div/span[2]/label').click()
                time.sleep(1)
                driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div[2]/div/div[1]/div').click()
                time.sleep(10)
                driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div[3]/div/div/div[1]/div').click()
                time.sleep(5)
                driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div[1]/div/div/div[1]/div/span[3]/label').click()
                time.sleep(2)
                driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div[2]/div/div[1]/div').click()
                time.sleep(5)
                driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div/div/div/div[2]/div/div/div/a').click()
                time.sleep(5)
                table = driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[2]/div/div[3]/div/div[3]/table/tbody')
                for order in table.find_elements_by_tag_name('tr'):
                    elem = order.find_elements_by_tag_name('td')[4]
                    action = ActionChains(driver)
                    action.double_click(elem).perform()

                    time.sleep(20)
                    driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div[1]/div/div/div[3]/div').click()
                    time.sleep(5)
                    driver.find_element_by_xpath('/html/body/div[2]/div[4]/div/div/div[1]/div').click()
                    time.sleep(3)
                    driver.find_element_by_xpath('/html/body/div[2]/div[5]/div/div/div[3]/div/div/div/div/div/div/div/div[2]/div/div/div/div/span[2]/label').click()
                    driver.find_element_by_xpath('/html/body/div[2]/div[5]/div/div/div[3]/div/div/div/div/div/div/div/div[3]/div/div/div[1]/div').click()
                    time.sleep(10)
                    driver.find_element_by_xpath('/html/body/div[2]/div[5]/div/div/div[3]/div/div/div/div/div/div/div/div/div/div/div[2]/div/div/div/a').click()
                    time.sleep(5)
                    driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/div/div[2]/div[2]').click()
                    time.sleep(3)

            driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[1]/div/div/div[5]/div/span').click()
            time.sleep(1)
            driver.find_element_by_xpath('/html/body/div[2]/div[2]/div/div/span').click()
            time.sleep(5)
        except Exception as e:
            logger.debug("the execution stopped abruptly")
            logger.debug(e)
        finally:
            display.stop()

        data = list()
        for file in glob.glob(os.path.join(download_folder, '*rden*.csv')):
            with open(file, 'r', encoding='latin1') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    data.append(dict(row))
            os.remove(file)

        for file in glob.glob(os.path.join(download_folder, 'Etiqueta*.xls')):
            read_file = pd.read_excel(file)
            read_file.to_csv(file.replace('.xls', '.csv'), index=None, header=True)
            os.remove(file)

        tags = dict()
        for file in glob.glob(os.path.join(download_folder, 'Etiqueta*.csv')):
            with open(file, 'r') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    hash = str(row['PLU']) + str(row['Orden'])
                    if hash not in tags:
                        tags[hash] = dict(row)
            os.remove(file)

        filter_data = list()
        for i in data:
            new_data = i.copy()
            hash = str(i['PLU']) + str(i['No OC'])
            if hash in tags.keys():
                new_data['Preci Venta'] = tags[hash]['Precio Venta']
                new_data['Cód. Barra'] = tags[hash]['Cód. Barra']
                new_data['Articulo'] = tags[hash]['Articulo']
                if new_data not in filter_data:
                    filter_data.append(new_data)

        return filter_data

    def detalle_venta_manual(self):
        download_folder = os.path.join(os.environ['SOURCE_INT_PATH'], f'b2b-files/{self.PORTAL}/raw/manual-files')
        data = list()
        for file in glob.glob(os.path.join(download_folder, '*rden*.csv')):
            with open(file, 'r', encoding='latin1') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    data.append(dict(row))
            fcsv = pathlib.Path(file)
            if datetime.datetime.now().today().timestamp() - fcsv.stat().st_mtime > self.FILE_TIME_OF_LIFE:
                os.remove(file)

        for file in glob.glob(os.path.join(download_folder, 'Etiqueta*.xls')):
            read_file = pd.read_excel(file)
            read_file.to_csv(file.replace('.xls', '.csv'), index=None, header=True)
            fxls = pathlib.Path(file)
            if datetime.datetime.now().today().timestamp() - fxls.stat().st_mtime > self.FILE_TIME_OF_LIFE:
                os.remove(file)

        tags = dict()
        for file in glob.glob(os.path.join(download_folder, 'Etiqueta*.csv')):
            with open(file, 'r') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    hash = str(row['PLU']) + str(row['Orden'])
                    if hash not in tags:
                        tags[hash] = dict(row)
            os.remove(file)

        filter_data = list()
        for i in data:
            new_data = i.copy()
            hash = str(i['PLU']) + str(i['No OC'])
            new_data['Precio Venta'] = tags[hash]['Precio Venta']
            new_data['Cód. Barra'] = tags[hash]['Cód. Barra']
            new_data['Articulo'] = tags[hash]['Articulo']
            if new_data not in filter_data:
                filter_data.append(new_data)

        return filter_data
