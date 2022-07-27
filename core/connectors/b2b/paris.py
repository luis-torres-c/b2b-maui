import csv
import glob
import os
import time
import pathlib
import datetime

from selenium.webdriver.common.keys import Keys
from seleniumwire import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.by import By
from pyvirtualdisplay import Display
from webdriver_manager.chrome import ChromeDriverManager

from core.connectors.b2b.base import BBReCommerceFileConnector
from core.connectors.b2b.base import BBReCommercePortalConnector
from core.connectors.b2b.base import BBReCommerceStockFileConnector
from utils.logger import logger


class ParisB2BConnector(BBReCommercePortalConnector):
    PORTAL = 'Paris'

    UNIQUE_PARAMS = {
        'unidad_negocio': '//*[@id="unidad_negocio"]/option[3]',
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
        'Estacion Selector': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[2]/div/div[4]/div/div/div[3]/div/input',
        'Search Button': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[5]/div/div/div',
        'Download Click 1': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div/div[2]/div/div/div/div/div[1]/div/div/div[3]/div/div[1]/div',
        'Download Click 2': '/html/body/div[2]/div[2]/div/div/div[3]/div',
        'Download Click 3': '/html/body/div[2]/div[3]/div/div/div[3]/div/div/div[3]/div/div[1]/div',
        'Close Download Popup': '/html/body/div[2]/div[3]/div/div/div[2]/div[2]',
        'Logout Click 1': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[1]/div/div/div[5]/div/span/span[2]',
        'Logout Click 2': '/html/body/div[2]/div[2]/div/div/span/span'
    }


class ParisB2BFileConnector(BBReCommerceFileConnector):

    fixed_sub_folder = 'Paris'

    def parse_metrics(self, row):
        row['VTA_PERIODO(u)'] = float(row['VTA_PERIODO(u)'])
        row['VTA_PERIODO_PUBLICO($)'] = float(row['VTA_PERIODO_PUBLICO($)'])
        row['VTA_PERIODO_COSTO($)'] = float(row['VTA_PERIODO_COSTO($)'])


class ParisB2BStockFileConnector(BBReCommerceStockFileConnector):

    fixed_sub_folder = 'Paris'

    def parse_metrics(self, row):
        if 'INVENTARIO(u)' in row:
            row['INVENTARIO(u)'] = float(row['INVENTARIO(u)'])
            row['INVENTARIO($)'] = float(row['INVENTARIO($)'])


class ParisB2BOCConnector(ParisB2BConnector):
    FILE_TIME_OF_LIFE = 82800

    def detalle_venta(self):
        data = list()
        source_int_path = os.environ['SOURCE_INT_PATH']
        download_folder = os.path.join(source_int_path, f'b2b-files/{self.PORTAL}/raw')

        display = Display(visible=not bool(os.environ.get('WIVO_ENV', False)))

        try:
            display.start()
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

            OC_Filter = ['Órdenes Vigentes', 'Próximamente Vigentes']

            for filter in OC_Filter:
                # click menu logistca
                driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[1]/div/div/div[3]/div/span[5]').click()
                time.sleep(3)
                # click ordenes y compra
                driver.find_element_by_xpath('/html/body/div[2]/div[2]/div/div/span[1]').click()
                time.sleep(10)
                
                # if exist thie xpaht:
                elements = driver.find_elements_by_xpath('/html/body/div[2]/div[3]/div/div/span[1]')
                if elements:
                    driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/div/span[1]').click()
                    time.sleep(10)

                if len(driver.find_elements_by_xpath('/html/body/div[2]/div[3]/div/div/div[2]/div[2]')):
                    driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/div/div[2]/div[2]').click()
                    time.sleep(5)

                driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[3]/div/div/div/div/div[2]/div/div/div[3]/div/input').click()
                time.sleep(5)

                for sellection in driver.find_element_by_xpath('/html/body/div[2]/div[2]').find_elements_by_tag_name('td'):
                    if filter in sellection.text:
                        sellection.click()
                        time.sleep(1)
                        break

                driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[1]/div/div[2]/div/div/div[3]/div/input').click()
                time.sleep(3)
                emp = {'b2b_empresa': self.b2b_empresa}
                self.get_relative_span(driver.find_element_by_xpath('/html/body/div[2]/div[2]/div/div[2]/table').find_elements_by_tag_name('td'), **emp).click()
                time.sleep(1)

                button_generar_informe = driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[4]/div/div/div')
                button_generar_informe.click()
                time.sleep(10)

                if driver.find_elements_by_xpath('/html/body/div[2]/div[3]/div/div/div[3]/div/div/div[3]/div/div[3]/div'):
                    driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/div/div[3]/div/div/div[3]/div/div[3]/div').click()
                    time.sleep(2)
                else:
                    driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[2]/div/div[1]/div/div/div[3]/div/div[1]/div').click()
                    time.sleep(5)
                    driver.find_element_by_xpath('/html/body/div[2]/div[2]/div/div/div[2]').click()
                    time.sleep(5)
                    driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div[2]/div/div/div/div/span[2]/label').click()
                    driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div[3]/div/div/div[1]/div').click()
                    time.sleep(2)
                    label_xpath = '/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div[1]/div/div/div[1]/div/span[3]/label'
                    WebDriverWait(driver, 10).until(
                        expected_conditions.element_to_be_clickable((By.XPATH, label_xpath))).click()
                    driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div[2]/div/div[1]').click()
                    time.sleep(30)
                    driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div/div/div/div[2]/div/div/div/a').click()
                    time.sleep(5)

            driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[1]/div/div/div[5]/div').click()
            time.sleep(1)
            driver.find_element_by_xpath('/html/body/div[2]/div[2]/div/div/span').click()
            time.sleep(2)

        except Exception as e:
            logger.error("the execution stopped abruptly")
            logger.error(e)
            display.sendstop()
        finally:
            display.sendstop()

        for file in glob.glob(os.path.join(download_folder, '*rden*.csv')):
            with open(file, 'r', encoding='ISO-8859-3') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    data.append(dict(row))
            os.remove(file)

        filter_data = list()
        for i in data:
            if i not in filter_data:
                filter_data.append(i)

        return filter_data

    def detalle_venta_manual(self):
        source_int_path = os.environ['SOURCE_INT_PATH']
        download_folder = os.path.join(source_int_path, f'b2b-files/{self.PORTAL}/raw/manual-files')
        data = list()
        for file in glob.glob(os.path.join(download_folder, '*rden*.csv')):
            with open(file, 'r', encoding='ISO-8859-3') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    data.append(dict(row))
            fcsv = pathlib.Path(file)
            if datetime.datetime.now().today().timestamp() - fcsv.stat().st_mtime > self.FILE_TIME_OF_LIFE:
                os.remove(file)

        filter_data = list()
        for i in data:
            if i not in filter_data:
                filter_data.append(i)

        return filter_data
