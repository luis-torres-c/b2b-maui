from core.sources.b2b.base import B2BPortalSource, B2BFileSource
from core.connectors.b2b.tottus import TottusB2BPortalConnector
from core.connectors.b2b.tottus import TottusB2BFileConnector
from core.connectors.b2b.tottus import TottusB2BStockConnector


class TottusB2BPortalSource(B2BPortalSource):

    CONNECTOR = TottusB2BPortalConnector

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class TottusB2BFileBaseSource(B2BFileSource):

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['b2b_empresa'] = kwargs['b2b_empresa']
        args['repository_path'] = kwargs['repository_path']
        return args


class TottusB2BFileSource(TottusB2BFileBaseSource):

    CONNECTOR = TottusB2BFileConnector


class TottusB2BStockSource(TottusB2BFileBaseSource):

    CONNECTOR = TottusB2BStockConnector
