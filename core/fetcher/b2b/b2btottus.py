import os

from core.sources.b2b.b2btottus import TottusB2BPortalSource
from core.sources.b2b.b2btottus import TottusB2BFileSource
from core.sources.b2b.b2btottus import TottusB2BStockSource
from core.fetcher.b2b.base import B2BPortalBase
from core.fetcher.base import B2BWebFetcher
from core.fetcher.b2b.b2bfalabella import FalabellaB2BSales
from core.fetcher.b2b.b2bfalabella import FalabellaB2BStockFile


class TottusB2BSales(TottusB2BFileSource, FalabellaB2BSales):

    name = 'b2b-tottus-files'

    PORTAL = 'Tottus'

    string_variables = {
        'username': 'B2B_USERNAME_TOTTUS',
        'empresa': 'B2B_EMPRESA_TOTTUS',
        'repository_path': 'SOURCE_INT_PATH',
    }


class TottusB2BStock(TottusB2BStockSource, FalabellaB2BStockFile):

    name = 'b2b-tottus-stock-files'

    PORTAL = 'Tottus'

    string_variables = {
        'username': 'B2B_USERNAME_TOTTUS',
        'empresa': 'B2B_EMPRESA_TOTTUS',
        'repository_path': 'SOURCE_INT_PATH',
        'stock_iva': 'STOCK_IVA_TOTTUS',
    }


class TottusB2BPortal(
        B2BPortalBase,
        TottusB2BPortalSource,
        B2BWebFetcher):

    name = 'b2b-tottus-portal'
    related_fetchers = [TottusB2BSales]

    string_variables = {
        'username': 'B2B_USERNAME_TOTTUS',
        'repository_path': 'SOURCE_INT_PATH',
        'password': 'B2B_PASSWORD_TOTTUS',
        'empresa': 'B2B_EMPRESA_TOTTUS',
    }

    @classmethod
    def settings(cls):
        args = super().settings()
        args['b2b_empresa'] = os.environ.get(cls.string_variables['empresa'], '')
        return args
