import requests
import csv
import io
import pathlib
import datetime
import os
import glob
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from core.connectors.b2b.base import B2BConnector
from core.connectors.b2b.utils import do_request
from core.connectors.b2b.utils import ConnectorB2BLoginErrorConnection, SaveErrorLogWhenBadCredentials
from utils.logger import logger


class RipleyB2BConnector(B2BConnector):
    BASE_URL = 'https://b2b.ripley.cl'
    LOGIN_PATH = '/b2bWeb/portal/logon.do'
    PROVEDOR_PATH = '/b2bWeb/portal/setProveedor.do'
    CONSULTA_PATH = '/b2bWeb/portal/comercial/consulta/ConsDetalladaVentasBuscar.do'

    FILE_NAME_PATH = 'b2b-files/{portal}/ventas/{year}/{month}/{filetype}_{client}_{empresa}_{date_from}_{date_to}_{timestamp}.csv'

    @classmethod
    def get_instance(cls, **kwargs):
        b2b_username = kwargs['b2b_username']
        b2b_password = kwargs['b2b_password']
        b2b_empresa = kwargs['b2b_empresa']
        return cls(b2b_username, b2b_password, b2b_empresa)

    def __init__(self, b2b_username, b2b_password, b2b_empresa):
        self.b2b_username = b2b_username
        self.b2b_password = b2b_password
        self.b2b_empresa = b2b_empresa

    def login(self, sessions_requests):
        self.check_login_credentials(self.b2b_username, self.b2b_password)

        url_login = self.BASE_URL + self.LOGIN_PATH
        url_provedores = self.BASE_URL + self.PROVEDOR_PATH

        payload_login = {
            "txtCodUsuario": self.b2b_username,
            "txtPassword": self.b2b_password,
        }
        logger.info('Trying login for {}'.format(self.b2b_username))

        do_request(url_login, sessions_requests, 'GET')
        login = do_request(url_login, sessions_requests, 'POST', payload_login)

        conection = str(login.headers.get("Set-Cookie"))
        if "OK" in conection:
            response = do_request(url_provedores, sessions_requests, 'GET')
            conection = str(response.headers.get("Set-Cookie"))
            if 'codigo' in conection:
                return login
            elif 'Debe seleccionar un proveedor o ingresar su RUT' in response.text:
                # Cuando una cuenta tiene más que un proveedor
                payload_empresa = {
                    'txtRutEmpresa': self.b2b_empresa
                }
                login = do_request(
                    url_provedores,
                    sessions_requests,
                    'POST',
                    payload_empresa)
                return login
            else:
                logger.error("Login error")
                SaveErrorLogWhenBadCredentials(response.content, os.environ['SOURCE_INT_PATH'], 'Ripley')
                arg = {'username': self.b2b_username, 'portal': 'Ripley'}
                raise ConnectorB2BLoginErrorConnection(arg)
        else:
            logger.error("Login error")
            SaveErrorLogWhenBadCredentials(login.content, os.environ['SOURCE_INT_PATH'], 'Ripley')
            arg = {'username': self.b2b_username, 'portal': 'Ripley'}
            raise ConnectorB2BLoginErrorConnection(arg)

    def generate_files(self, **kwargs):
        url_consulta = self.BASE_URL + self.CONSULTA_PATH

        sessions_requests = requests.session()
        self.login(sessions_requests)

        periods_checked = list()

        date_format = '%Y-%m-%d'
        date_from = datetime.datetime.strptime(kwargs['from'], date_format)
        date_to = datetime.datetime.strptime(kwargs['to'], date_format)

        while date_from <= date_to:

            do_request(url_consulta, sessions_requests, 'GET')

            payload_consulta = {
                "hdnFechaDesde": date_from.strftime("%d-%m-%Y"),
                "hdnFechaHasta": date_from.strftime("%d-%m-%Y"),
                "cboTipoConsulta": "0",
                "txtFechaDesde": date_from.strftime("%d-%m-%Y"),
                "txtFechaHasta": date_from.strftime("%d-%m-%Y"),
                "cboSucursalDestino": "",
                "cboMarca": "",
                "cboDepto": "",
                "cboLinea": "",
                "cboTemporada": "",
                "txtCodArtRipley": "",
                "txtCodArtProveedor": "",
                "txtCodUpc": "",
                "accion": "buscar",
                "chkFile": "on",
            }

            response = do_request(
                url_consulta,
                sessions_requests,
                'POST',
                payload_consulta)
            soup = BeautifulSoup(response.text, 'html.parser')

            if not soup.find("div", {"class": "NormalText"}):
                logger.warning("Cannot reach url")
                href_csv = None
            else:
                href_csv = soup.find("div", {"class": "NormalText"}).a['href']

            if not href_csv:
                date_from = date_from + datetime.timedelta(days=1)
                continue

            csv_url = self.BASE_URL + href_csv

            response = do_request(
                csv_url,
                sessions_requests,
                'GET')

            today_date = datetime.datetime.today()
            file_name = self.FILE_NAME_PATH.format(
                portal='Ripley',
                filetype='ventas',
                client=self.b2b_username.replace('_', ''),
                empresa=self.b2b_empresa,
                year=today_date.strftime('%Y'),
                month=today_date.strftime('%m'),
                date_from=date_from.strftime("%Y-%m-%d"),
                date_to=date_from.strftime("%Y-%m-%d"),
                timestamp=today_date.timestamp(),
            )

            file_full_path = os.path.join(kwargs['source_int_path'], file_name)
            os.makedirs(os.path.dirname(file_full_path), exist_ok=True)
            logger.info("Saving file {}".format(file_full_path))
            with open(file_full_path, 'w') as csvfile:
                csvfile.write(response.text.strip())

            if self.check_if_csv(file_full_path):
                periods_checked.append({
                    'from': date_from.strftime(date_format),
                    'to': date_from.strftime(date_format),
                    'status': 'ok'
                })
            else:
                logger.warning("It was an error creating file {}, trying again..".format(file_full_path))
                os.remove(file_full_path)

            date_from = date_from + datetime.timedelta(days=1)

        return periods_checked


class RipleyB2BFileBaseConnector(B2BConnector):

    file_pattern = 'ventas_{b2b_username}_{b2b_empresa}_*.csv'
    fixed_sub_folder = 'Ripley'
    fixed_sub_root_folder = 'b2b-files'
    fixed_sub_sales_folder = 'ventas'

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
            logger.info("File to process {}".format(file_name))
            return (file_name, ts_file)
        else:
            logger.warning("No files to process!")
            return (None, None)


class RipleyB2BFileConnector(RipleyB2BFileBaseConnector):

    def get_file_name(self):
        return self.find_file('ventas')

    def parse_metric(self, row):
        row['Venta Valorizada($)'] = float(row['Venta Valorizada($)'])
        row['Unidades Vendidas'] = float(row['Unidades Vendidas'])
        row['Stock on Hand ($)'] = float(row['Stock on Hand ($)'])
        row['Stock on Hand (u)'] = float(row['Stock on Hand (u)'])

    def detalle_venta(self):
        data = []
        file_name, ts_file = self.get_file_name()

        if file_name:
            with open(file_name, 'r') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    new_row = row
                    new_row['Datetime'] = self.date_start
                    self.parse_metric(new_row)
                    data.append(new_row)
        return data


class RipleyB2BStockFileConnector(RipleyB2BFileConnector):

    def get_file_name(self):
        return self.find_file('ventas', per_timestamp=True)


class RipleyB2BOCConnector(RipleyB2BConnector):
    OC_PATH = '/b2bWeb/portal/comercial/consulta/OrdenCompraHistoricaBuscar.do'
    FILE_TIME_OF_LIFE = 82000

    @classmethod
    def get_instance(cls, **kwargs):
        b2b_username = kwargs['b2b_username']
        b2b_password = kwargs['b2b_password']
        date_start = kwargs['date_start']
        date_start = date_start.strftime("%d-%m-%Y")
        b2b_empresa = kwargs['b2b_empresa']
        return cls(b2b_username, b2b_password, b2b_empresa, date_start)

    def __init__(self, b2b_username, b2b_password, b2b_empresa, date_start):
        self.b2b_username = b2b_username
        self.b2b_password = b2b_password
        self.date_start = date_start
        self.b2b_empresa = b2b_empresa

    def detalle_venta(self):
        url_consulta = self.BASE_URL + self.OC_PATH

        sessions_requests = requests.session()

        login = self.login(sessions_requests)

        if not login:
            arg = {'username': self.b2b_username,
                   'portal': 'Ripley'}
            raise ConnectorB2BLoginErrorConnection(arg)

        data = []

        
        date_end = datetime.datetime.now().strftime("%d-%m-%Y")
        date_start = (datetime.datetime.now() - relativedelta(days=5)).strftime("%d-%m-%Y")
        logger.info(f"dowloading from {date_start} to {date_end}")
        payload_consulta = {
            'optFecha': '0',
            'txtFechaDesde': date_start,
            'txtFechaHasta': date_end,
            'cboMetodoDistribucion': '',
            'cboTipoOCA': '',
            'cboEstado': '',
            'txtNumOCA': '',
            'chkArchivo': 'on',
            'btnGeneral': 'Buscar',
            'buscar': 'true',
        }

        response = do_request(
            url_consulta,
            sessions_requests,
            'POST',
            payload_consulta)
        soup = BeautifulSoup(response.text, 'html.parser')

        try:
            href_csv = soup.find("div", {"class": "NormalText"}).a['href']
        except AttributeError:
            logger.warning("No data in this day")
            return data

        if not href_csv:
            logger.warning("No data in this day")
            return data

        csv_url = self.BASE_URL + href_csv

        response = do_request(
            csv_url,
            sessions_requests,
            'GET')

        foo = response.content.decode('latin1')
        reader = csv.DictReader(io.StringIO(foo))
        for row in reader:
            data.append(row)
        return data

    def detalle_venta_manual(self):
        download_folder = os.path.join(os.environ['SOURCE_INT_PATH'], f'b2b-files/Ripley/raw/manual-files')
        data = []
        for file in glob.glob(os.path.join(download_folder, '*.csv')):
            with open(file, encoding='latin1') as f:
                reader = csv.DictReader(f)
                data = list(reader)
                f.close()
            fcsv = pathlib.Path(file)
            if datetime.datetime.now().today().timestamp() - fcsv.stat().st_mtime > self.FILE_TIME_OF_LIFE:
                os.remove(file)
        return data


class RipleyB2BPeruConnector(RipleyB2BConnector):

    BASE_URL = 'https://b2b.ripley.com.pe'


class RipleyB2BOCTestConnector(RipleyB2BOCConnector):

    BASE_URL = 'http://trackingqa.ripley.cl'