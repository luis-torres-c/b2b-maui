from core.sources.b2b.base import B2BFileSource
from core.sources.b2b.base import B2BPortalSource
from core.connectors.b2b.easy import EasyB2BConnector
from core.connectors.b2b.easy import EasyB2BFileConnector
from core.connectors.b2b.easy import EasyB2BStockFileConnector


class EasyB2BPortalSource(B2BPortalSource):

    CONNECTOR = EasyB2BConnector

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class EasyB2BBaseSource(B2BFileSource):

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['repository_path'] = kwargs['repository_path']
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class EasyB2BFileSource(EasyB2BBaseSource):

    CONNECTOR = EasyB2BFileConnector


class EasyB2BStockSource(EasyB2BBaseSource):

    CONNECTOR = EasyB2BStockFileConnector
