from core.sources.b2b.base import B2BPortalSource, B2BFileSource
from core.connectors.b2b.sodimac import SodimacB2BPortalConnector
from core.connectors.b2b.sodimac import SodimacFileConnector
from core.connectors.b2b.sodimac import SodimacB2BStockConnector


class SodimacB2BPortalSource(B2BPortalSource):

    CONNECTOR = SodimacB2BPortalConnector

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class SodimacB2BFileBaseSource(B2BFileSource):

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['b2b_empresa'] = kwargs['b2b_empresa']
        args['repository_path'] = kwargs['repository_path']
        return args


class SodimacB2BFileSource(SodimacB2BFileBaseSource):

    CONNECTOR = SodimacFileConnector


class SodimacB2BStockSource(SodimacB2BFileBaseSource):

    CONNECTOR = SodimacB2BStockConnector
