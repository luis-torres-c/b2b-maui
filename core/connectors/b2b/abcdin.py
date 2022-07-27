import time

from core.connectors.b2b.base import BBReCommercePortalConnector
from core.connectors.b2b.base import BBReCommerceFileConnector
from core.connectors.b2b.base import BBReCommerceStockFileConnector
# from core.connectors.b2b.base import SalesInventory


class AbcdinB2BConnector(BBReCommercePortalConnector):
    PORTAL = 'Abcdin'
    POST_URL = 'https://portalb2b.abcdin.cl/ABCDin/BBRe-commerce/main/UIDL/?v-uiId=0'

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
        driver.get("https://portalb2b.abcdin.cl/")
        time.sleep(4)


class AbcdinB2BFileConnector(BBReCommerceFileConnector):

    fixed_sub_folder = 'Abcdin'

    def parse_metrics(self, row):
        row['VTA_PERIODO(u)'] = float(row['VTA_PERIODO(u)'])
        row['VTA_PERIODO($)'] = float(row['VTA_PERIODO($)'])


class AbcdinB2BStockFileConnector(BBReCommerceStockFileConnector):

    fixed_sub_folder = 'Abcdin'

    def parse_metrics(self, row):
        if 'INVENTARIO(u)' in row:
            row['INVENTARIO(u)'] = float(row['INVENTARIO(u)'])
            row['INVENTARIO($)'] = float(row['INVENTARIO($)'])
