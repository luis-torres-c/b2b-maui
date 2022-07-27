from core.connectors.b2b.unimarc import UnimarcB2BFileConnector
from core.connectors.b2b.unimarc import UnimarcB2BStockFileConnector
from core.connectors.b2b.unimarc import UnimarcB2BWebConnector

from core.sources.b2b.base import B2BFileSource
from core.sources.b2b.base import B2BPortalSource


class UnimarcB2BPortalSource(B2BPortalSource):

    CONNECTOR = UnimarcB2BWebConnector

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class UnimarcB2BBaseSource(B2BFileSource):

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['repository_path'] = kwargs['repository_path']
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class UnimarcB2BFileSource(UnimarcB2BBaseSource):

    CONNECTOR = UnimarcB2BFileConnector


class UnimarcB2BStockSource(UnimarcB2BBaseSource):

    CONNECTOR = UnimarcB2BStockFileConnector
