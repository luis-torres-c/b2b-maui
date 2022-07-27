# from core.fetcher.b2b.b2bcorona import CoronaB2BOC
from core.fetcher.b2b.b2bfalabella import FalabellaB2BCustomSales
from core.fetcher.b2b.b2bfalabella import FalabellaB2BOC
from core.fetcher.b2b.b2bfalabella import FalabellaB2BPortal
from core.fetcher.b2b.b2bhites import HitesB2B
from core.fetcher.b2b.b2bhites import HitesB2BCustomSales
from core.fetcher.b2b.b2bhites import HitesB2BOC
from core.fetcher.b2b.b2blapolar import LaPolarB2B
from core.fetcher.b2b.b2blapolar import LaPolarB2BCustomSales
from core.fetcher.b2b.b2blapolar import LaPolarB2BOC
from core.fetcher.b2b.b2bparis import ParisB2B
from core.fetcher.b2b.b2bparis import ParisB2BCustomSales
import os

from core.fetcher.b2b.b2bparis import ParisB2BOC
from core.fetcher.b2b.b2bripley import RipleyB2BCustomSales
from core.fetcher.b2b.b2bripley import RipleyB2BOC
from core.fetcher.b2b.b2bripley import RipleyB2BPortal
from core.fetcher.b2b.b2bripley import RipleyB2BOCTest


class B2BMauiRipley(RipleyB2BOC):

    name = 'maui-oc-ripley'
    DELIMITER = ';'

    @classmethod
    def settings(cls):
        username = os.environ.get('RIPLEY_B2B_USERNAME', '')
        password = os.environ.get('RIPLEY_B2B_PASSWORD', '')
        empresa = os.environ.get('RIPLEY_B2B_EMPRESA', '')
        storage_path = os.environ['STORAGE_PATH']
        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': empresa,
        }

    def parse_data(self, connector, **kwargs):
        results = super().parse_data(connector, **kwargs)
        for result in results:
            for row in result['records']:
                row['EMPRESA'] = kwargs['b2b_empresa']
        return results


class B2BMauiRipleyManual(RipleyB2BOC):

    name = 'maui-oc-ripley-manual'
    DELIMITER = ';'

    @classmethod
    def settings(cls):
        username = os.environ.get('RIPLEY_B2B_USERNAME', '')
        password = os.environ.get('RIPLEY_B2B_PASSWORD', '')
        empresa = os.environ.get('RIPLEY_B2B_EMPRESA', '')
        storage_path = os.environ['STORAGE_PATH']
        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': empresa,
        }

    def parse_data(self, connector, **kwargs):
        results = super().parse_data_manual(connector, **kwargs)
        for result in results:
            for row in result['records']:
                row['EMPRESA'] = kwargs['b2b_empresa']
        return results


class B2BMauiRipleyTest(RipleyB2BOCTest):

    name = 'maui-oc-ripley-test'
    DELIMITER = ';'

    @classmethod
    def settings(cls):
        username = os.environ.get('RIPLEY_B2B_USERNAME', '')
        password = os.environ.get('RIPLEY_B2B_PASSWORD', '')
        empresa = os.environ.get('RIPLEY_B2B_EMPRESA', '')
        storage_path = os.environ['STORAGE_PATH']
        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': empresa,
        }

    def parse_data(self, connector, **kwargs):
        results = super().parse_data(connector, **kwargs)
        for result in results:
            for row in result['records']:
                row['EMPRESA'] = kwargs['b2b_empresa']
        return results


class B2BMauiRipleyRip(RipleyB2BOC):

    name = 'maui-oc-ripley-rip'
    DELIMITER = ';'

    @classmethod
    def settings(cls):
        username = os.environ.get('RIPLEY_RIP_B2B_USERNAME', '')
        password = os.environ.get('RIPLEY_RIP_B2B_PASSWORD', '')
        empresa = os.environ.get('RIPLEY_RIP_B2B_EMPRESA', '')
        storage_path = os.environ['STORAGE_PATH']
        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': empresa,
        }

    def parse_data(self, connector, **kwargs):
        results = super().parse_data(connector, **kwargs)
        for result in results:
            for row in result['records']:
                row['EMPRESA'] = kwargs['b2b_empresa']
        return results


class B2BMauiRipleyProsurf(RipleyB2BOC):

    name = 'maui-oc-ripley-prosurf'
    DELIMITER = ';'

    @classmethod
    def settings(cls):
        username = os.environ.get('RIPLEY_PROSURF_B2B_USERNAME', '')
        password = os.environ.get('RIPLEY_PROSURF_B2B_PASSWORD', '')
        storage_path = os.environ['STORAGE_PATH']
        empresa = os.environ.get('RIPLEY_RIP_B2B_EMPRESA', '')
        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': empresa,
        }

    def parse_data(self, connector, **kwargs):
        results = super().parse_data(connector, **kwargs)
        for result in results:
            for row in result['records']:
                row['EMPRESA'] = kwargs['b2b_empresa']
        return results


class B2BMauiFalabella(FalabellaB2BOC):

    name = 'maui-oc-falabella'
    DELIMITER = ';'

    @classmethod
    def settings(cls):
        username = os.environ.get('FALABELLA_B2B_USERNAME', '')
        password = os.environ.get('FALABELLA_B2B_PASSWORD', '')
        storage_path = os.environ['STORAGE_PATH']
        b2b_empresa = os.environ.get('FALABELLA_B2B_EMPRESA', '')

        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': b2b_empresa,
        }

    def parse_data(self, connector, **kwargs):
        results = super().parse_data(connector, **kwargs)
        for result in results:
            for row in result['records']:
                row['EMPRESA'] = kwargs['b2b_empresa']
        return results


class B2BMauiFalabellaManual(FalabellaB2BOC):

    name = 'maui-oc-falabella-manual'
    DELIMITER = ';'

    @classmethod
    def settings(cls):
        username = os.environ.get('FALABELLA_B2B_USERNAME', '')
        password = os.environ.get('FALABELLA_B2B_PASSWORD', '')
        storage_path = os.environ['STORAGE_PATH']
        b2b_empresa = os.environ.get('FALABELLA_B2B_EMPRESA', '')

        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': b2b_empresa,
        }

    def parse_data(self, connector, **kwargs):
        results = super().parse_data_manual(connector, **kwargs)
        for result in results:
            for row in result['records']:
                row['EMPRESA'] = kwargs['b2b_empresa']
        return results

    def detalle_venta(self):

        data, od_data = self.extract_data_from_files()
        od_data = self.preprocessed_data(od_data=od_data, data=data)

        # data, od_data = self.extract_data()
        # od_data = self.preprocessed_data(od_data=od_data, data=data)

        return od_data


class B2BMauiFalabellaRip(FalabellaB2BOC):

    name = 'maui-oc-falabella-rip'
    DELIMITER = ';'

    @classmethod
    def settings(cls):
        username = os.environ.get('FALABELLA_RIP_B2B_USERNAME', '')
        password = os.environ.get('FALABELLA_RIP_B2B_PASSWORD', '')
        storage_path = os.environ['STORAGE_PATH']
        b2b_empresa = os.environ.get('FALABELLA_RIP_B2B_EMPRESA', '')

        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': b2b_empresa,
        }

    def parse_data(self, connector, **kwargs):
        results = super().parse_data(connector, **kwargs)
        for result in results:
            for row in result['records']:
                row['EMPRESA'] = kwargs['b2b_empresa']
        return results


class B2BMauiFalabellaProsurf(FalabellaB2BOC):

    name = 'maui-oc-falabella-prosurf'
    DELIMITER = ';'

    @classmethod
    def settings(cls):
        username = os.environ.get('FALABELLA_PROSURF_B2B_USERNAME', '')
        password = os.environ.get('FALABELLA_PROSURF_B2B_PASSWORD', '')
        storage_path = os.environ['STORAGE_PATH']
        b2b_empresa = os.environ.get('FALABELLA_PROSURF_B2B_EMPRESA', '')

        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': b2b_empresa,
        }

    def parse_data(self, connector, **kwargs):
        results = super().parse_data(connector, **kwargs)
        for result in results:
            for row in result['records']:
                row['EMPRESA'] = kwargs['b2b_empresa']
        return results


class B2BMauiParis(ParisB2BOC):

    name = 'maui-oc-paris'
    DELIMITER = ';'

    @classmethod
    def settings(cls):
        username = os.environ.get('PARIS_B2B_USERNAME', '')
        password = os.environ.get('PARIS_B2B_PASSWORD', '')
        storage_path = os.environ['STORAGE_PATH']
        b2b_empresa = os.environ.get('PARIS_B2B_EMPRESA', '')

        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': b2b_empresa,
        }

    def parse_data(self, connector, **kwargs):
        connector.VENDORID = 21672
        results = super().parse_data(connector, **kwargs)
        for result in results:
            for row in result['records']:
                row['EMPRESA'] = 'Maui'
        return results


class B2BMauiParisManual(ParisB2BOC):

    name = 'maui-oc-paris-manual'
    DELIMITER = ';'

    @classmethod
    def settings(cls):
        username = os.environ.get('PARIS_B2B_USERNAME', '')
        password = os.environ.get('PARIS_B2B_PASSWORD', '')
        storage_path = os.environ['STORAGE_PATH']
        b2b_empresa = os.environ.get('PARIS_B2B_EMPRESA', '')

        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': b2b_empresa,
        }

    def parse_data(self, connector, **kwargs):
        connector.VENDORID = 21672
        results = super().parse_data_manual(connector, **kwargs)
        for result in results:
            for row in result['records']:
                row['EMPRESA'] = 'Maui'
        return results


class B2BMauiParisRip(ParisB2BOC):

    name = 'maui-oc-paris-rip'

    DELIMITER = ';'

    @classmethod
    def settings(cls):
        username = os.environ.get('PARIS_RIP_B2B_USERNAME', '')
        password = os.environ.get('PARIS_RIP_B2B_PASSWORD', '')
        storage_path = os.environ['STORAGE_PATH']
        b2b_empresa = os.environ.get('PARIS_RIP_B2B_EMPRESA', '')

        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': b2b_empresa,
        }

    def parse_data(self, connector, **kwargs):
        connector.VENDORID = 225985
        results = super().parse_data(connector, **kwargs)
        for result in results:
            for row in result['records']:
                row['EMPRESA'] = 'Rip'
        return results


class B2BMauiParisProsurf(ParisB2BOC):

    name = 'maui-oc-paris-prosurf'

    DELIMITER = ';'

    @classmethod
    def settings(cls):
        username = os.environ.get('PARIS_B2B_USERNAME', '')
        password = os.environ.get('PARIS_B2B_PASSWORD', '')
        storage_path = os.environ['STORAGE_PATH']
        b2b_empresa = os.environ.get('PARIS_B2B_EMPRESA', '')

        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': b2b_empresa,
        }

    def parse_data(self, connector, **kwargs):
        connector.VENDORID = 39263280
        results = super().parse_data(connector, **kwargs)
        for result in results:
            for row in result['records']:
                row['EMPRESA'] = 'Prosurf'
        return results


class B2BMauiLaPolar(LaPolarB2BOC):

    DELIMITER = ';'

    name = 'maui-oc-lapolar'

    def parse_data(self, connector, **kwargs):
        results = super().parse_data(connector, **kwargs)
        for result in results:
            for row in result['records']:
                row['EMPRESA'] = '965555102'
        return results

class B2BMauiLaPolarManual(LaPolarB2BOC):

    DELIMITER = ';'

    name = 'maui-oc-lapolar-manual'

    def parse_data(self, connector, **kwargs):
        results = super().parse_data_manual(connector, **kwargs)
        for result in results:
            for row in result['records']:
                row['EMPRESA'] = '965555102'
        return results

#
# class B2BMauiCorona(CoronaB2BOC):
#
#     DELIMITER = ';'
#
#     name = 'maui-oc-corona'
#
#     def parse_data(self, connector, **kwargs):
#         results = super().parse_data(connector, **kwargs)
#         for result in results:
#             for row in result['records']:
#                 row['EMPRESA'] = kwargs['b2b_username']
#         return results


class B2BMauiHites(HitesB2BOC):

    name = 'maui-oc-hites'

    DELIMITER = ';'

    def parse_data(self, connector, **kwargs):
        connector.VENDORID = 1400
        results = super().parse_data(connector, **kwargs)
        for result in results:
            for row in result['records']:
                row['EMPRESA'] = '96555510'
        return results

class B2BMauiHitesManual(HitesB2BOC):

    name = 'maui-oc-hites-manual'

    DELIMITER = ';'

    def parse_data(self, connector, **kwargs):
        connector.VENDORID = 1400
        results = super().parse_data_manual(connector, **kwargs)
        for result in results:
            for row in result['records']:
                row['EMPRESA'] = '96555510'
        return results


class B2BMauiRipleyPortalCustomSales(RipleyB2BPortal):

    name = 'maui-custom-sales-ripley-portal'


class B2BMauiRipleyCustomSales(RipleyB2BCustomSales):

    name = 'maui-custom-sales-ripley'
    DELIMITER = ';'

    @classmethod
    def settings(cls):
        username = os.environ.get('RIPLEY_B2B_USERNAME', '')
        password = os.environ.get('RIPLEY_B2B_PASSWORD', '')
        empresa = os.environ.get('RIPLEY_B2B_EMPRESA', '')
        storage_path = os.environ['STORAGE_PATH']
        repository_path = os.environ.get(cls.string_variables['repository_path'])
        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': empresa,
            'repository_path': repository_path,
        }


class B2BMauiRipleyRipPortalCustomSales(RipleyB2BPortal):

    name = 'maui-custom-sales-ripley-rip-portal'


class B2BMauiRipleyRipCustomSales(RipleyB2BCustomSales):

    name = 'maui-custom-sales-ripley-rip'
    DELIMITER = ';'

    @classmethod
    def settings(cls):
        username = os.environ.get('RIPLEY_RIP_B2B_USERNAME', '')
        password = os.environ.get('RIPLEY_RIP_B2B_PASSWORD', '')
        empresa = os.environ.get('RIPLEY_RIP_B2B_EMPRESA', '')
        storage_path = os.environ['STORAGE_PATH']
        repository_path = os.environ.get(cls.string_variables['repository_path'])
        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': empresa,
            'repository_path': repository_path,
        }


class B2BMauiRipleyProsurfPortalCustomSales(RipleyB2BPortal):

    name = 'maui-custom-sales-ripley-prosurf-portal'


class B2BMauiRipleyProsurfCustomSales(RipleyB2BCustomSales):

    name = 'maui-custom-sales-ripley-prosurf'
    DELIMITER = ';'

    @classmethod
    def settings(cls):
        username = os.environ.get('RIPLEY_PROSURF_B2B_USERNAME', '')
        password = os.environ.get('RIPLEY_PROSURF_B2B_PASSWORD', '')
        storage_path = os.environ['STORAGE_PATH']
        empresa = os.environ.get('RIPLEY_RIP_B2B_EMPRESA', '')
        repository_path = os.environ.get(cls.string_variables['repository_path'])
        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': empresa,
            'repository_path': repository_path,
        }


class B2BMauiFalabellaCustomSales(FalabellaB2BCustomSales):

    name = 'maui-custom-sales-falabella'
    DELIMITER = ';'

    @classmethod
    def settings(cls):
        username = os.environ.get('FALABELLA_B2B_USERNAME', '')
        password = os.environ.get('FALABELLA_B2B_PASSWORD', '')
        storage_path = os.environ['STORAGE_PATH']
        b2b_empresa = os.environ.get('FALABELLA_B2B_EMPRESA', '')
        repository_path = os.environ.get(cls.string_variables['repository_path'])
        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': b2b_empresa,
            'repository_path': repository_path,
        }


class B2BMauiFalabellaPortalCustomSales(FalabellaB2BPortal):

    name = 'maui-custom-sales-falabella-portal'


class B2BMauiFalabellaRipCustomSales(FalabellaB2BCustomSales):

    name = 'maui-custom-sales-falabella-rip'
    DELIMITER = ';'

    @classmethod
    def settings(cls):
        username = os.environ.get('FALABELLA_RIP_B2B_USERNAME', '')
        password = os.environ.get('FALABELLA_RIP_B2B_PASSWORD', '')
        storage_path = os.environ['STORAGE_PATH']
        b2b_empresa = os.environ.get('FALABELLA_RIP_B2B_EMPRESA', '')
        repository_path = os.environ.get(cls.string_variables['repository_path'])
        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': b2b_empresa,
            'repository_path': repository_path,
        }


class B2BMauiFalabellaPortalRipCustomSales(FalabellaB2BPortal):

    name = 'maui-custom-sales-falabella-rip-portal'


class B2BMauiFalabellaProsurfCustomSales(FalabellaB2BCustomSales):

    name = 'maui-custom-sales-falabella-prosurf'
    DELIMITER = ';'

    @classmethod
    def settings(cls):
        username = os.environ.get('FALABELLA_PROSURF_B2B_USERNAME', '')
        password = os.environ.get('FALABELLA_PROSURF_B2B_PASSWORD', '')
        storage_path = os.environ['STORAGE_PATH']
        b2b_empresa = os.environ.get('FALABELLA_PROSURF_B2B_EMPRESA', '')
        repository_path = os.environ.get(cls.string_variables['repository_path'])
        return {
            'storage_path': storage_path,
            'b2b_username': username,
            'b2b_password': password,
            'b2b_empresa': b2b_empresa,
            'repository_path': repository_path,
        }


class B2BMauiFalabellaPortalProsurfCustomSales(FalabellaB2BPortal):

    name = 'maui-custom-sales-falabella-prosurf-portal'


class B2BMauiParisPortalCustomSales(ParisB2B):

    name = 'maui-custom-sales-paris-portal'


class B2BMauiParisCustomSales(ParisB2BCustomSales):

    name = 'maui-custom-sales-paris'
    DELIMITER = ';'


class B2BMauiParisPortalRipCustomSales(ParisB2B):

    name = 'maui-custom-sales-paris-rip-portal'


class B2BMauiParisRipCustomSales(ParisB2BCustomSales):

    name = 'maui-custom-sales-paris-rip'

    DELIMITER = ';'


class B2BMauiParisPortalProsurfCustomSales(ParisB2B):

    name = 'maui-custom-sales-paris-prosurf-portal'


class B2BMauiParisProsurfCustomSales(ParisB2BCustomSales):

    name = 'maui-custom-sales-paris-prosurf'

    DELIMITER = ';'


class B2BMauiLaPolarPortalCustomSales(LaPolarB2B):

    name = 'maui-custom-sales-lapolar-portal'


class B2BMauiLaPolarCustomSales(LaPolarB2BCustomSales):

    DELIMITER = ';'

    name = 'maui-custom-sales-lapolar'


class B2BMauiHitesPortalCustomSales(HitesB2B):

    name = 'maui-custom-sales-hites-portal'


class B2BMauiHitesCustomSales(HitesB2BCustomSales):

    name = 'maui-custom-sales-hites'

    DELIMITER = ';'
