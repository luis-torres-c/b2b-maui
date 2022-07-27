import csv

from core.connectors.b2b.base import BBReCommercePortalConnector
from core.connectors.b2b.base import BBReCommerceFileBaseConnector


class NewJumboB2BConnector(BBReCommercePortalConnector):
    PORTAL = 'Jumbo'
    POST_URL = 'https://www.cenconlineb2b.com/SuperCL/BBRe-commerce/main/UIDL/?v-uiId=0'

    UNIQUE_PARAMS = {
        'unidad_negocio': '//*[@id="unidad_negocio"]/option[6]',
        'popup_messages': [
            '/html/body/div[2]/div[6]/div/div/div[2]/div[2]',
            '/html/body/div[2]/div[4]/div/div/div[2]/div[2]',
            '/html/body/div[2]/div[2]/div/div/div[2]/div[2]'
        ],
        'Comercial Menu': '//*[@id="SuperCLBBRecommercemain-347102763"]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[1]/div/div/div[3]/div/span[3]',
        'Informe de Ventas': '//*[@id="SuperCLBBRecommercemain-347102763-overlays"]/div[2]/div/div/span[1]',
        'Empresa Selector': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[1]/div/div[2]/div/div/div[3]/div/input',
        'Span Options': '/html/body/div[2]',
        'Calendar From': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[5]/div/div[2]/div/div/div[1]/div/div[3]/div/button',
        'Calendar To': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[5]/div/div[2]/div/div/div[5]/div/div[3]/div/button',
        'Estacion Selector': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[4]/div/div[2]/div/div/div[3]/div/input',
        'Search Button': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div/div/div[6]/div/div/div',
        'Download Click 1': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[3]/div/div/div/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div/div[2]/div/div/div/div/div[1]/div/div/div[3]/div/div[1]/div',
        'Download Click 2': '/html/body/div[2]/div[2]/div/div/div[2]/div',
        'Download Click 3': '/html/body/div[2]/div[3]/div/div/div[3]/div/div/div[3]/div/div[1]/div',
        'Close Download Popup': '/html/body/div[2]/div[3]/div/div/div[2]/div[2]',
        'Logout Click 1': '/html/body/div[1]/div/div[2]/div/div/div/div/div/div/div[2]/div/div/div[1]/div/div/div[5]/div/span/span[2]',
        'Logout Click 2': '/html/body/div[2]/div[2]/div/div/span'
    }


class NewJumboB2BFileMixin(BBReCommerceFileBaseConnector):

    def complement_data(self, data):
        filename, timestamp = self.find_file('products', last_file=True)
        products = dict()

        if not filename:
            return data

        with open(filename, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                products[row['Cód. Cencosud']] = {
                    'Código Categoría': row['Código Categoría'],
                    'Descripción Categoría': row['Descripción Categoría']
                }

        no_category = {
            'Código Categoría': 'NC',
            'Descripción Categoría': 'NO CLASIFICADO'
        }

        new_data = list()
        for row in data:
            if row['COD_CENCOSUD'] in products:
                new_data.append({**row, **products[row['COD_CENCOSUD']]})
            else:
                new_data.append({**row, **no_category})
        return new_data


class NewJumboB2BFileConnector(NewJumboB2BFileMixin):
    fixed_sub_folder = 'Jumbo'

    def parse_metrics(self, row):
        row['VENTA(Un)'] = float(row['VENTA(Un)'])
        row['CONTRIBUCION'] = float(row['CONTRIBUCION'])
        if 'VTA_PUBLICO($)' in row:
            row['VTA_PUBLICO($)'] = float(row['VTA_PUBLICO($)'])
        elif 'VENTA_PUBLICO($)' in row:
            row['VTA_PUBLICO($)'] = float(row['VENTA_PUBLICO($)'])

    def get_file_name(self):
        return self.find_file('ventas')


class NewJumboB2BStockFileConnector(NewJumboB2BFileMixin):
    fixed_sub_folder = 'Jumbo'

    def parse_metrics(self, row):
        if 'INV_ACTUAL(Un)' in row:
            row['INV_ACTUAL(Un)'] = float(row['INV_ACTUAL(Un)'])
            row['INV_ACTUAL_COSTO($)'] = float(row['INV_ACTUAL_COSTO($)'])

    def get_file_name(self):
        return self.find_file('ventas', per_timestamp=True)
