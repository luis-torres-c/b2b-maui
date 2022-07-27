import csv
import os
import time
import glob
import pathlib
import datetime

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.by import By
from pyvirtualdisplay import Display

from core.connectors.b2b.base import B2BConnector
from core.connectors.b2b.base import BBReCommerceFileConnector
from core.connectors.b2b.base import BBReCommercePortalConnector
from core.connectors.b2b.base import BBReCommerceStockFileConnector
from core.connectors.b2b.base import OrderCriteriaDTO
from core.connectors.b2b.base import SalesInventory
from utils.recaptcha import resolve_recaptcha
from utils.logger import logger


class HitesB2BOCConnector(B2BConnector):
    BASE_URL = 'https://www.hitesb2b.com/'
    FILE_TIME_OF_LIFE = 82000

    @classmethod
    def get_instance(cls, **kwargs):
        b2b_username = kwargs['b2b_username']
        b2b_password = kwargs['b2b_password']

        return cls(b2b_username, b2b_password)

    def parse_metrics(self, new_row):
        pass

    def __init__(self,
                 b2b_username,
                 b2b_password
                 ):

        self.b2b_username = b2b_username
        self.b2b_password = b2b_password

    def login(self, driver):
        self.check_login_credentials(self.b2b_username, self.b2b_password)

        driver.get(self.BASE_URL)
        driver.refresh()
        time.sleep(5)
        driver.find_element_by_id("username").send_keys(self.b2b_username)
        driver.find_element_by_id("password").send_keys(self.b2b_password)
        sitekey = driver.find_element_by_class_name('g-recaptcha').get_attribute('data-sitekey')
        result = resolve_recaptcha(sitekey, driver.current_url)
        driver.execute_script(f'document.getElementById("g-recaptcha-response").value = "{result}"')
        driver.find_element_by_id("kc-login").click()
        logger.debug("Waiting portal to login")
        time.sleep(30)

    def detalle_venta(self, **kwargs):
        download_folder = os.path.join(kwargs['repository_path'], 'b2b-files/Hites/raw')

        with Display(visible=0):
            from selenium.webdriver.chrome.options import Options

            options = Options()
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_experimental_option("prefs", {
                "download.default_directory": download_folder,
                "download.prompt_from_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True
            })

            driver = webdriver.Chrome(ChromeDriverManager().install(),
                                      service_args=['--verbose', '--log-path=/tmp/chromedriver.log'],
                                      chrome_options=options)
            try:
                self.login(driver)

                # By roberto.suil@wivo.cl #
                # Seleccion de logistica apertura de menu

                # popup1 = '/html/body/div[2]/div[2]/div/div/div[2]/div[2]'
                # WebDriverWait(driver, 15).until(
                #     expected_conditions.element_to_be_clickable((By.XPATH, popup1))).click()
                logger.debug("Abriendo menu de logistica")
                span_xpath = '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[1]/div/div/div[3]/div/span[5]'
                WebDriverWait(driver, 15).until(
                    expected_conditions.element_to_be_clickable((By.XPATH, span_xpath))).click()
                time.sleep(3)

                # Seleccion de OC a hites
                logger.debug("Seleccion de boton OC a hites")
                driver.find_element_by_xpath('/html/body/div[2]/div[2]/div/div/span[1]').click()
                time.sleep(2)

                # Pulsar generar informe
                logger.debug("Pulsar generar informe")
                driver.find_element_by_xpath(
                    '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[4]/div/div/div').click()
                time.sleep(10)

                # Pulsar todos
                logger.debug("Pulsar todos")
                driver.find_element_by_xpath(
                    '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[2]/div/div[3]/div/div[3]/table/thead/tr/th[1]').click()
                time.sleep(2)

                # Pulsar boton descarga
                logger.debug("Pulsar descarga")
                driver.find_element_by_xpath(
                    'html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[2]/div/div[1]/div/div/div[3]/div/div[1]/div').click()
                time.sleep(3)

                # Pulsar descarga general
                logger.debug("Pulsar 'descarga general'")
                driver.find_element_by_css_selector(
                    '#BBRecommercemain-1422079705-overlays > div.bbr-popupbutton-popup.v-popupview-popup.bbr-popupbutton-popup-toolbar-button > div > div > div:nth-child(1)').click()
                time.sleep(3)

                # Pulsar descargar
                logger.debug("Pulsar descargar")
                driver.find_element_by_xpath(
                    '/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div[3]/div/div[1]/div').click()
                time.sleep(3)

                # Seleccion CSV
                logger.debug("Seleccionar formato csv")
                driver.find_element_by_xpath(
                    '/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div[2]/div/div/div/div/span[2]').click()
                time.sleep(3)

                # Pulsar seleccion
                logger.debug("Pulsar seleccion")
                driver.find_element_by_xpath(
                    '/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div[3]/div/div/div[1]/div').click()
                time.sleep(3)

                # Seleccion descarga
                logger.debug("Seleccionar descarga")
                driver.find_element_by_xpath(
                    '/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div/div/div/div[2]/div/div/div').click()
                time.sleep(3)

                # Pulsar boton descarga
                logger.debug("Pulsar boton descarga")
                driver.find_element_by_xpath(
                    'html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[2]/div/div[1]/div/div/div[3]/div/div[1]/div').click()
                time.sleep(3)

                # pulsar descargar etiqueta
                logger.debug("Pulsar descargar etiqueta")
                driver.find_element_by_css_selector(
                    '#BBRecommercemain-1422079705-overlays > div.bbr-popupbutton-popup.v-popupview-popup.bbr-popupbutton-popup-toolbar-button > div > div > div:nth-child(4) > div').click()
                time.sleep(20)

                # pulsar descargar
                logger.debug("Pulsar descargar")
                driver.find_element_by_xpath(
                    '/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div[2]/div/div[1]/div').click()
                time.sleep(3)

                # seleccionCSV
                logger.debug("Seleccionar formato CSV")
                driver.find_element_by_xpath(
                    '/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div[2]/div/div/div/div/span[2]').click()
                time.sleep(3)

                # pulsarseleccion
                logger.debug("Pulsar la seleccion")
                driver.find_element_by_xpath(
                    '/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div[3]/div/div/div[1]/div').click()
                time.sleep(3)

                # seleccionydescarga
                logger.debug("Descarga de archivo")
                driver.find_element_by_xpath(
                    '/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div/div/div/div[2]/div/div/div').click()
                time.sleep(5)

                # selecciónmenuusuario
                logger.debug("Desplegar menu de usuario")
                driver.find_element_by_xpath(
                    '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[1]/div/div/div[5]/div').click()
                time.sleep(2)

                # cerrarsession
                logger.debug("Cerrar Sesion")
                driver.find_element_by_xpath('/html/body/div[2]/div[2]/div/div/span').click()
                time.sleep(3)
                    # End Roberto Suil #
            except Exception as e:
                logger.error("there was an error downloading the files")
                logger.error(e)
            driver.quit()

        data = []
        etiquetas = dict()
        ordenes = list()
        files = glob.glob(os.path.join(download_folder, '*.csv'))
        for f in files:
            logger.info(f"Find {f}")
            if 'Etiquetas' in f:
                with open(f, 'r', encoding='ISO-8859-15') as csvfile:
                    reader = csv.DictReader(x.replace('\0', '') for x in csvfile)
                    for r in reader:
                        etiquetas[r['Cód. de Barras']] = r
            elif 'Órdenes' in f or 'Orden' in f:
                with open(f, 'r', encoding='ISO-8859-15') as csvfile:
                    reader = csv.DictReader(x.replace('\0', '') for x in csvfile)
                    for r in reader:
                        ordenes.append(r)
            os.remove(f)

        for orden in ordenes:
            new_data = orden.copy()
            etiqueta = etiquetas[new_data['Código Barra']]
            new_data.update(etiqueta)
            data.append(new_data)

        for d in data:
            d['Fecha Entrega'] = d['Fecha Entrega'].split(' al ')[0] if 'Fecha Entrega' in d else ''

        return data

    def detalle_venta_manual(self, **kwargs):
        download_folder = os.path.join(kwargs['repository_path'], 'b2b-files/Hites/raw/manual-files')
        data = []
        etiquetas = dict()
        ordenes = list()
        files = glob.glob(os.path.join(download_folder, '*.csv'))
        for f in files:
            logger.info(f"Find {f}")
            if 'Etiquetas' in f:
                with open(f, 'r', encoding='ISO-8859-15') as csvfile:
                    reader = csv.DictReader(x.replace('\0', '') for x in csvfile)
                    for r in reader:
                        etiquetas[r['Cód. de Barras']] = r
            elif 'Órdenes' in f or 'Orden' in f:
                with open(f, 'r', encoding='ISO-8859-15') as csvfile:
                    reader = csv.DictReader(x.replace('\0', '') for x in csvfile)
                    for r in reader:
                        ordenes.append(r)
            fcsv = pathlib.Path(f)
            if datetime.datetime.now().today().timestamp() - fcsv.stat().st_mtime > self.FILE_TIME_OF_LIFE:
                os.remove(f)


        for orden in ordenes:
            new_data = orden.copy()
            etiqueta = etiquetas[new_data['Código Barra']]
            new_data.update(etiqueta)
            data.append(new_data)

        for d in data:
            d['Fecha Entrega'] = d['Fecha Entrega'].split(' al ')[0] if 'Fecha Entrega' in d else ''

        return data


class OrderDetailReportInitParamDTO:

    def __init__(self, orderId, vendorId, pageNumber=1, orderby=[OrderCriteriaDTO(propertyname='OCNUMBER', ascending=True)], rows=50):
        self.pageNumber = pageNumber
        self.orderId = orderId
        self.vendorId = vendorId
        self.orderby = orderby
        self.rows = rows


class HitesB2BConnector(BBReCommercePortalConnector):
    BASE_URL = 'https://www.hitesb2b.com/Hites/BBRe-commerce/'
    LOGIN_PATH = 'access/login.do'
    REFERER_PATH = 'swf/HITES_CL.swf?v=2.4.8&sitecode=BBR-HITES/[[DYNAMIC]]/5'

    server_class_name = 'bbr.b2b.hites.commercial.sales.data.classes.DownloadSalesInventoryReportInitParamDTO'

    PORTAL = 'Hites'

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
        self.b2b_empresa = str()
        self.payload = SalesInventory(
            levelProduct=0,
            groupByProduct=False,
            showInventory=True,
            levelLocal=0,
            excludeProductsWithoutSales=False,
            productOrCategory=False,
            typeOfMark=-1,
            season='-1',
            catLocRetailer=True,
            levelToGroupProduct=-1,
            keyLocal=-1,
            formatType='csv',
            viewProductOrLocal=False,
            levelToGroupLocal=-1,
            typeOfLocal=-1,
            activeProducts=False,
            keyProduct=-1,
            levelToGroup=-1,
            groupByLocal=True,
            activeLocals=False,
            catProdRetailer=True,
            excludeProductsWithoutInventory=False,
            localOrCategory=False)


class HitesB2BFileConnector(BBReCommerceFileConnector):

    fixed_sub_folder = 'Hites'

    def parse_metrics(self, row):
        row['VTA_PERIODO(u)'] = float(row['VTA_PERIODO(u)'])
        row['VTA_PUBLICO($)'] = float(row['VTA_PUBLICO($)'])
        row['VTA_COSTO($)'] = float(row['VTA_COSTO(u)'])


class HitesB2BStockFileConnector(BBReCommerceStockFileConnector):

    fixed_sub_folder = 'Hites'

    def parse_metrics(self, row):
        if 'INVENTARIO(u)' in row:
            row['INVENTARIO(u)'] = float(row['INVENTARIO(u)'])
            row['INVENTARIO($)'] = float(row['INVENTARIO($)'])
