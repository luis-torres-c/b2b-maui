import requests
import csv
import io
import datetime
import os
import glob
from bs4 import BeautifulSoup
from babel.numbers import parse_decimal
from core.connectors.b2b.base import B2BConnector
from core.connectors.b2b.utils import do_request
from core.connectors.b2b.utils import ConnectorB2BLoginErrorConnection, SaveErrorLogWhenBadCredentials
from utils.logger import logger


class MaicaoB2BConnector(B2BConnector):
    BASE_URL = 'http://wc1.difarma.cl:9090'
    LOGIN_PATH = '/apex/f?p=102:1'
    API_PATH = '/apex/wwv_flow.accept'
    DOWNLOAD_PATH = '/apex/f?p={}:{}:{}:CSV:'

    FILE_NAME_PATH = 'b2b-files/{portal}/ventas/{year}/{month}/{filetype}_{client}_{empresa}_{date_from}_{date_to}_{timestamp}.csv'

    @classmethod
    def get_instance(cls, **kwargs):
        b2b_username = kwargs['b2b_username']
        b2b_password = kwargs['b2b_password']
        return cls(b2b_username, b2b_password)

    def __init__(self, b2b_username, b2b_password):
        self.b2b_username = b2b_username
        self.b2b_password = b2b_password

    def login(self, sessions_requests):
        self.check_login_credentials(self.b2b_username, self.b2b_password)

        url_login = self.BASE_URL + self.LOGIN_PATH
        url_post_login = self.BASE_URL + self.API_PATH

        logger.debug("Get data for login")
        response = do_request(url_login, sessions_requests, 'GET')

        soup = BeautifulSoup(response.text, 'html.parser')

        # Make form imput to login
        form_data = {
            'p_arg_names': []
        }
        for input_tag in soup.findAll('input'):
            if input_tag.attrs['name'] == 'p_arg_names':
                form_data['p_arg_names'].append(input_tag.attrs['value'])
            else:
                form_data[input_tag.attrs['name']] = input_tag.attrs['value']
        form_data['p_t01'] = self.b2b_username
        form_data['p_t02'] = self.b2b_password
        form_data['p_request'] = 'LOGIN'

        response = do_request(url_post_login, sessions_requests, 'POST', form_data)
        soup = BeautifulSoup(response.text, 'html.parser')
        if soup.find('div', {'class': 'app-user'}) and 'Bienvenido' in soup.find('div', {'class': 'app-user'}).text:
            return soup

        logger.error("Login error")
        SaveErrorLogWhenBadCredentials(response.content, os.environ['SOURCE_INT_PATH'], 'Maicao')
        arg = {'username': self.b2b_username, 'portal': 'Maicao'}
        raise ConnectorB2BLoginErrorConnection(arg)

    def logout(self, sessions_requests, flow_id):
        LOGOUT_PATH = '/apex/wwv_flow_custom_auth_std.logout?p_this_flow={}&p_next_flow_page_sess={}:1'
        logout_url = self.BASE_URL + LOGOUT_PATH.format(flow_id, flow_id)
        do_request(logout_url, sessions_requests, 'GET')

    def generate_files(self, **kwargs):
        month = {
            '01': 'Ene',
            '02': 'Feb',
            '03': 'Mar',
            '04': 'Abr',
            '05': 'May',
            '06': 'Jun',
            '07': 'Jul',
            '08': 'Ago',
            '09': 'Sep',
            '10': 'Oct',
            '11': 'Nov',
            '12': 'Dic',
        }

        url_api = self.BASE_URL + self.API_PATH
        actual_date = datetime.date.today()

        sessions_requests = requests.session()

        login = self.login(sessions_requests)

        form_data = {
            'p_flow_id': login.find('input', {'name': 'p_flow_id'}).attrs['value'],
            'p_flow_step_id': login.find('input', {'name': 'p_flow_step_id'}).attrs['value'],
            'p_instance': login.find('input', {'name': 'p_instance'}).attrs['value'],
            'p_page_submission_id': login.find('input', {'name': 'p_page_submission_id'}).attrs['value'],
            'p_request': 'T_VENTAS',
            'p_md5_checksum': '',
        }

        response = do_request(url_api, sessions_requests, 'POST', form_data)
        soup = BeautifulSoup(response.text, 'html.parser')
        form_data = {
            'p_flow_id': soup.find('input', {'name': 'p_flow_id'}).attrs['value'],
            'p_flow_step_id': soup.find('input', {'name': 'p_flow_step_id'}).attrs['value'],
            'p_instance': soup.find('input', {'name': 'p_instance'}).attrs['value'],
            'p_page_submission_id': soup.find('input', {'name': 'p_page_submission_id'}).attrs['value'],
            'p_request': 'P2_VTA_SUC',
            'p_md5_checksum': '',
            'p_arg_names': [i.attrs['value'] for i in soup.findAll('input', {'name': 'p_arg_names'})],
            'p_t01': actual_date.year,
            'p_t02': actual_date.month,
            'p_t03': '',
            'p_t04': (actual_date - datetime.timedelta(days=1)).strftime('%d/%m/%Y'),
        }
        response = do_request(url_api, sessions_requests, 'POST', form_data)
        soup = BeautifulSoup(response.text, 'html.parser')

        datetime_from = datetime.datetime.strptime(kwargs['from'], '%Y-%m-%d')
        datetime_to = datetime.datetime.strptime(kwargs['to'], '%Y-%m-%d')
        from_str_date = '{}-{}-{}'.format(datetime_from.strftime('%d'), month[datetime_from.strftime('%m')], datetime_from.strftime('%Y'))
        to_str_date = '{}-{}-{}'.format(datetime_to.strftime('%d'), month[datetime_to.strftime('%m')], datetime_to.strftime('%Y'))
        form_data = {
            'p_flow_id': soup.find('input', {'name': 'p_flow_id'}).attrs['value'],
            'p_flow_step_id': soup.find('input', {'name': 'p_flow_step_id'}).attrs['value'],
            'p_instance': soup.find('input', {'name': 'p_instance'}).attrs['value'],
            'p_page_submission_id': soup.find('input', {'name': 'p_page_submission_id'}).attrs['value'],
            'p_request': 'con',
            'p_md5_checksum': '',
            'p_arg_names': [i.attrs['value'] for i in soup.findAll('input', {'name': 'p_arg_names'})],
            'p_t01': from_str_date,
            'p_t02': to_str_date,
            'p_t03': '',
            'p_t04': '',
            'p_t05': '',
            'p_t06': '',
            'p_t07': '',
        }
        response = do_request(url_api, sessions_requests, 'POST', form_data)
        download_url = self.BASE_URL + self.DOWNLOAD_PATH.format(form_data['p_flow_id'], form_data['p_flow_step_id'], form_data['p_instance'])

        response = do_request(download_url, sessions_requests, 'GET')

        foo = response.content.decode('latin1')
        reader = csv.DictReader(io.StringIO(foo), delimiter=';')

        if not reader.fieldnames:
            logger.debug("No data in this period")
            return []

        today_date = datetime.datetime.today()
        file_name = self.FILE_NAME_PATH.format(
            portal='Maicao',
            filetype='ventas',
            client=self.b2b_username.replace('_', ''),
            empresa='maicao',
            year=today_date.strftime('%Y'),
            month=today_date.strftime('%m'),
            date_from=datetime_from.strftime("%Y-%m-%d"),
            date_to=datetime_to.strftime("%Y-%m-%d"),
            timestamp=today_date.timestamp(),
        )

        file_full_path = os.path.join(kwargs['source_int_path'], file_name)
        os.makedirs(os.path.dirname(file_full_path), exist_ok=True)
        logger.debug("Saving file {}".format(file_full_path))

        with open(file_full_path, 'w') as csvfile:
            csvfile.write(response.text.strip())

        self.logout(sessions_requests, form_data['p_flow_id'])

        return [{'from': kwargs['from'], 'to': kwargs['to'], 'status': 'ok'}]


class MaicaoB2BFileBaseConnector(B2BConnector):

    file_pattern = 'ventas_{b2b_username}_{b2b_empresa}_*.csv'
    fixed_sub_folder = 'Maicao'
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
        self.user_name = b2b_username.replace('_', '')
        self.b2b_empresa = 'maicao'
        self.repository_path = os.path.join(
            base_path, self.fixed_sub_root_folder, self.fixed_sub_folder, self.fixed_sub_sales_folder)

    def find_file(self, filetype, per_timestamp=False):
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
            if date_from <= self._date and self._date <= date_to:
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


class MaicaoB2BFileConnector(MaicaoB2BFileBaseConnector):

    month = {
        '01': 'ENE',
        '02': 'FEB',
        '03': 'MAR',
        '04': 'ABR',
        '05': 'MAY',
        '06': 'JUN',
        '07': 'JUL',
        '08': 'AGO',
        '09': 'SEP',
        '10': 'OCT',
        '11': 'NOV',
        '12': 'DIC',
    }

    def detalle_venta(self):
        data = []
        file_name, ts_file = self.find_file('ventas')

        base_date = '{}-{}-{}'.format(self._date.strftime("%d"), self.month[self._date.strftime("%m")], self._date.strftime("%Y"))

        if file_name:
            with open(file_name, 'r') as csvfile:
                reader = csv.DictReader(csvfile, delimiter=';')
                for row in reader:
                    if row['Fecha Venta'] == base_date:
                        new_row = row
                        new_row['Datetime'] = self.date_start
                        new_row['Vta.Neta $'] = float(parse_decimal(row['Vta.Neta $'], locale='es_CL'))
                        data.append(new_row)
        return data
