import time

from core.connectors.b2b.base import BBReCommerceFileConnector
from core.connectors.b2b.base import BBReCommercePortalConnector
from core.connectors.b2b.base import BBReCommerceStockFileConnector


class UnimarcB2BFileConnector(BBReCommerceFileConnector):

    fixed_sub_folder = 'Unimarc'

    def parse_metrics(self, row):
        row['Vta. Unid.'] = float(row['Vta. Unid.'])
        row['Vta. Púb.(s/IVA)'] = float(row['Vta. Púb.(s/IVA)'])
        row['Vta. Costo(s/IVA)'] = float(row['Vta. Costo(s/IVA)'])


class UnimarcB2BStockFileConnector(BBReCommerceStockFileConnector):

    fixed_sub_folder = 'Unimarc'

    def parse_metrics(self, row):
        if 'Inventario(u)' in row:
            row['Inventario(u)'] = float(row['Inventario(u)'])
            row['Inv. a Costo(s/IVA)'] = float(row['Inv. a Costo(s/IVA)'])


class UnimarcB2BWebConnector(BBReCommercePortalConnector):
    PORTAL = 'Unimarc'
    POST_URL = 'https://b2b.smu.cl/BBRe-commerce/main/UIDL/?v-uiId=0'

    PERIODO_COLUMN_NAME = 'Período'

    ENCODING = 'latin1'

    UNIQUE_PARAMS = {
        'popup_messages': [
            '/html/body/div[2]/div[10]/div/div/div[2]/div[2]',
            '/html/body/div[2]/div[8]/div/div/div[2]/div[2]',
            '/html/body/div[2]/div[6]/div/div/div[2]/div[2]',
            '/html/body/div[2]/div[4]/div/div/div[2]/div[2]',
            '/html/body/div[2]/div[2]/div/div/div[2]/div[2]',
            '/html/body/div[2]/div[8]/div/div/div[2]/div[2]',
        ],
        'Comercial Menu': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[1]/div/div/div[3]/div/span[3]',
        'Informe de Ventas': '/html/body/div[2]/div[2]/div/div/span[2]/span',
        'Empresa Selector': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[1]/div/div[2]/div/div/div[3]/div/input',
        'Span Options': '/html/body/div[2]',
        'Calendar From': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[4]/div/div[1]/div/div[2]/div/div/div/div/div[1]/div/div[3]/div/button',
        'Calendar To': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[4]/div/div[1]/div/div[2]/div/div/div/div/div[2]/div/div[3]/div/button',
        'Estacion Selector': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[1]/div/div[2]/div/div/div[3]/div/input',
        'Search Button': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[5]/div/div/div',
        'Download Click 1': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div/div[2]/div/div/div/div/div[1]/div/div/div[3]/div/div[1]/div',
        'Download Click 2': '/html/body/div[2]/div[2]/div/div/div[2]',
        'Download Click 3': '/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div[2]/div/div/div/div/span[2]/label',
        'Download Click 4': '/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div[3]/div/div/div[1]/div',
        'Close Download Popup': '/html/body/div[2]/div[3]/div/div/div[2]/div[2]',
        'Logout Click 1': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[1]/div/div/div[5]/div/span',
        'Logout Click 2': '/html/body/div[2]/div[2]/div/div/span'
    }

    def login_form(self, driver):
        driver.get("https://b2b.smu.cl/")
        time.sleep(4)

    def download_clicks(self, driver):
        driver.find_element_by_xpath(self.UNIQUE_PARAMS['Download Click 1']).click()
        time.sleep(10)
        driver.find_element_by_xpath(self.UNIQUE_PARAMS['Download Click 2']).click()
        time.sleep(2)
        driver.find_element_by_xpath(self.UNIQUE_PARAMS['Download Click 3']).click()
        driver.find_element_by_xpath(self.UNIQUE_PARAMS['Download Click 4']).click()
        time.sleep(30)