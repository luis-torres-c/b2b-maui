import csv
import os
import time

from core.connectors.b2b.base import BBReCommercePortalConnector
from core.connectors.b2b.base import BBReCommerceFileBaseConnector

from utils.logger import logger


class EasyB2BConnector(BBReCommercePortalConnector):
    PORTAL = 'Easy'
    POST_URL = 'https://www.cenconlineb2b.com/EasyCL/BBRe-commerce/main/UIDL/?v-uiId=0'

    UNIQUE_PARAMS = {
        'unidad_negocio': '//*[@id="unidad_negocio"]/option[1]',
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
        'Download Click 3': '/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div[3]/div/div/div[1]/div',
        'Close Download Popup': '/html/body/div[2]/div[3]/div/div/div[2]/div[2]',
        'Logout Click 1': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[1]/div/div/div[5]/div/span',
        'Logout Click 2': '/html/body/div[2]/div[2]/div/div/span'
    }

    def extra_extraction(self, driver):
        logger.debug("Making extra extraction")
        driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[1]/div/div/div[3]/div/span[2]').click()
        time.sleep(5)
        driver.find_element_by_xpath('/html/body/div[2]/div[2]/div/div/span[1]').click()
        time.sleep(10)
        driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[3]/div/div/div').click()
        time.sleep(20)
        driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[2]/div/div[1]/div/div/div[3]/div/div[1]/div').click()
        time.sleep(5)
        driver.find_element_by_xpath('/html/body/div[2]/div[2]/div/div/div[1]').click()
        time.sleep(10)
        driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/div/div[3]/div/div/div/div/div/div/div/div[3]/div/div/div[1]/div').click()
        time.sleep(10)

        request_with_url = None
        for request in driver.requests:
            if request.response and request.response.body and 'FichaProductos' in request.response.body.decode('latin-1'):
                request_with_url = request

        body = request_with_url.response.body.decode('latin-1')
        url = body.split('"uRL":"')[1].split('"')[0]
        logger.debug(f"Making request to {url}")
        driver.get(url)

        time.sleep(3)
        driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/div/div[2]/div[2]').click()
        time.sleep(2)

    def check_stock_box(self, driver):
        # TODO: Resolve this
        pass

    def save_file(self, file, today_date, **kwargs):
        if 'Productos' in file:
            filetype = 'product'
            file_date = today_date.strftime('%Y-%m-%d')
        else:
            filetype = 'ventas'
            file_date = None
            for key in kwargs['file_downloaded']:
                filename_to_search = file.split('/')[-1].split('.csv')[0]
                if filename_to_search in key:
                    file_date = kwargs['file_downloaded'][key]
        if not file_date:
            logger.warning("No file_date founded")
            os.remove(file)
        else:
            file_name = self.FILE_NAME_PATH.format(
                portal=self.PORTAL,
                year=today_date.strftime('%Y'),
                month=today_date.strftime('%m'),
                filetype=filetype,
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


class EasyB2BFileMixin(BBReCommerceFileBaseConnector):

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

    def complement_data(self, data):
        filename, timestamp = self.find_file('product', last_file=True)
        products = dict()

        if not filename:
            return data

        no_category = dict()
        with open(filename, 'r', encoding=self.ENCODE) as csvfile:
            reader = csv.DictReader(x.replace('\0', '') for x in csvfile)
            for row in reader:
                products[row['Cód. Cencosud']] = row
                if not no_category:
                    for key in row.keys():
                        no_category[key] = ''

        new_data = list()
        for row in data:
            if row['Cód. Cencosud'] in products:
                new_data.append({**row, **products[row['Cód. Cencosud']]})
            else:
                new_data.append({**row, **no_category})
        return new_data

    def parse_metrics(self, row):
        to_float = ['Vta. Púb.($)', 'Vta. Período(u)', 'Inv.(u)', 'Inv. ($)']
        for key in to_float:
            if key in row:
                row[key] = float(row[key])


class EasyB2BFileConnector(EasyB2BFileMixin):

    fixed_sub_folder = 'Easy'

    def get_file_name(self):
        return self.find_file('ventas')


class EasyB2BStockFileConnector(EasyB2BFileMixin):

    fixed_sub_folder = 'Easy'

    def get_file_name(self):
        return self.find_file('ventas', per_timestamp=True)
