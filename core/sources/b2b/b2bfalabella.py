from core.sources.b2b.base import B2BPortalSource, B2BFileSource, B2BWebSource
from core.connectors.b2b.falabella import FalabellaB2BPortalConnector, FalabellaB2BFileConnector, FalabellaB2BStockConnector, FalabellaB2BPeruPortalConnector, FalabellaB2BPeruFileConnector, FalabellaB2BOCConnector


class FalabellaB2BOCWebSource(B2BWebSource):

    CONNECTOR = FalabellaB2BOCConnector

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class FalabellaB2BPortalSource(B2BPortalSource):

    CONNECTOR = FalabellaB2BPortalConnector

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class FalabellaB2BFileBaseSource(B2BFileSource):

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['b2b_empresa'] = kwargs['b2b_empresa']
        args['repository_path'] = kwargs['repository_path']
        return args


class FalabellaB2BFileSource(FalabellaB2BFileBaseSource):

    CONNECTOR = FalabellaB2BFileConnector


class FalabellaB2BStockSource(FalabellaB2BFileBaseSource):

    CONNECTOR = FalabellaB2BStockConnector


class FalabellaB2BPeruPortalSource(B2BPortalSource):

    CONNECTOR = FalabellaB2BPeruPortalConnector

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class FalabellaB2BPeruFileSource(FalabellaB2BFileBaseSource):

    CONNECTOR = FalabellaB2BPeruFileConnector
