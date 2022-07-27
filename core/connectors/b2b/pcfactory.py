import requests
import datetime
from bs4 import BeautifulSoup
from time import sleep
from core.connectors.b2b.base import B2BConnector
from core.connectors.b2b.utils import do_request
from core.connectors.b2b.utils import ConnectorB2BLoginErrorConnection
from core.connectors.b2b.utils import CaptchaErrorException
from utils.logger import logger


class PcFactoryB2BConnector(B2BConnector):
    BASE_URL = "https://b2b.pcfactory.cl/"
    VENTAS_PATH = "?ventas&d=R&suc&fecha_desde={}&fecha_hasta={}"
    STOCK_PATH = "?stock&suc"
    CAPTCHA_GET = "http://2captcha.com/in.php?key={}&method=userrecaptcha&googlekey={}&pageurl={}"
    CAPTCHA_ANSWER = "http://2captcha.com/res.php?key={}&action=get&id={}"

    DATE_FORMAT = '%s-%s-%s'  # Ej: 2018-12-30

    @classmethod
    def get_instance(cls, **kwargs):
        b2b_username = kwargs['b2b_username']
        b2b_password = kwargs['b2b_password']
        date_start = kwargs['date_start']
        captcha_key = kwargs['captcha_key']
        captcha_googlekey = kwargs['captcha_googlekey']
        return cls(
            b2b_username,
            b2b_password,
            date_start,
            captcha_key,
            captcha_googlekey,
        )

    def __init__(self,
                 b2b_username,
                 b2b_password,
                 date_start,
                 captcha_key,
                 captcha_googlekey
                 ):

        self.b2b_username = b2b_username
        self.b2b_password = b2b_password
        self.date_start = date_start
        self.captcha_key = captcha_key
        self.captcha_googlekey = captcha_googlekey
        self.fieldnames = []

    def login(self, sessions_requests):
        url_home = self.BASE_URL

        do_request(url_home, sessions_requests, 'GET')

        logger.info("Requests 2Captcha for resolving a captcha")
        captcha_id = sessions_requests.post(
            self.CAPTCHA_GET.format(
                self.captcha_key,
                self.captcha_googlekey,
                url_home)).text.split('|')[1]
        recaptcha_answer = sessions_requests.get(
            self.CAPTCHA_ANSWER.format(
                self.captcha_key, captcha_id)).text

        logger.info("solving ref captcha...")

        attempts = 0

        try:
            while 'CAPCHA_NOT_READY' in recaptcha_answer:
                logger.debug('Captcha not ready. Trying again... ')
                sleep(15)
                recaptcha_answer = sessions_requests.get(
                    self.CAPTCHA_ANSWER.format(
                        self.captcha_key, captcha_id)).text
                attempts += 1

                if attempts > 100:
                    raise CaptchaErrorException
        except BaseException:
            logger.error("Error solving captcha... trying again")
            return self.login(self, sessions_requests)

        recaptcha_answer = recaptcha_answer.split('|')[1]

        payload_login = {
            "usuario": self.b2b_username,
            "clave": self.b2b_password,
            "g-recaptcha-response": recaptcha_answer,
        }

        login = do_request(
            url_home,
            sessions_requests,
            'POST',
            payload_login,
            url_home)

        if login.status_code == 302 or b'Olvido clave?' in login.content:
            return None

        return login

    def detalle_venta(self, actual_date):

        sessions_requests = requests.session()

        login = self.login(sessions_requests)
        max_retries = 4
        tries = 1

        while not login:
            tries += 1
            if tries > max_retries:
                logger.error("Max retries for login")
                args = {
                    'username': self.b2b_username,
                    'portal': 'PcFactory',
                }
                raise ConnectorB2BLoginErrorConnection(args)
            logger.debug("Retrying login")
            login = self.login(sessions_requests)

        response = do_request(
            self.BASE_URL +
            self.STOCK_PATH,
            sessions_requests,
            'GET')
        soup = BeautifulSoup(response.text, 'html.parser')

        stock_rows = soup.find_all('div', attrs={'class': 'w3-row'})
        del stock_rows[0]
        data = []
        store_actual = dict()
        for stock in stock_rows:
            new_row = dict()
            if stock.find('div', attrs={'class': 'm12'}):
                new_row['type'] = 'stores'
                new_row['store_id'] = 'pcfactory' + \
                    stock.find('div', attrs={'class': 'm12'}).text.strip()
                new_row['store_name'] = stock.find(
                    'div', attrs={'class': 'm12'}).text.strip()
                data.append(new_row)
                store_actual = new_row
                new_row = dict()
            else:
                # Producto
                row = stock.find_all('div')
                new_row['type'] = 'b2bproduct'
                new_row['b2bproduct_id'] = 'pcfactory' + row[0].text.strip()
                new_row['b2bproduct_name'] = row[3].text.strip()
                data.append(new_row)
                new_row = dict()
                # Metrica
                new_row['type'] = 'b2bstockunit'
                new_row['value'] = row[4].text.strip()
                new_row['b2bproduct_id'] = 'pcfactory' + row[0].text.strip()
                new_row['store_id'] = store_actual['store_id']
                new_row['chain_id'] = 'pcfactory'
                data.append(new_row)
                new_row = dict()

        ayer = (actual_date - datetime.timedelta(days=1)).strftime("%y-%m-%d")
        response = do_request(
            self.BASE_URL +
            self.VENTAS_PATH.format(
                ayer,
                ayer),
            sessions_requests,
            'GET')
        soup = BeautifulSoup(response.text, 'html.parser')
        sales_rows = soup.find_all('div', attrs={'class': 'w3-row'})
        del sales_rows[0]
        for sales in sales_rows:
            new_row = dict()
            if sales.find('div', attrs={'class': 'm12'}):
                new_row['type'] = 'stores'
                new_row['store_id'] = 'pcfactory' + \
                    sales.find('div', attrs={'class': 'm12'}).text.strip()
                new_row['store_name'] = sales.find(
                    'div', attrs={'class': 'm12'}).text.strip()
                store_actual = new_row
                new_row = dict()
            else:
                row = sales.find_all('div')
                new_row['type'] = 'b2bsaleunits'
                new_row['value'] = row[4].text.strip()
                new_row['b2bproduct_id'] = 'pcfactory' + row[0].text.strip()
                new_row['store_id'] = store_actual['store_id']
                new_row['chain_id'] = 'pcfactory'
                data.append(new_row)
                new_row = dict()

        return data
