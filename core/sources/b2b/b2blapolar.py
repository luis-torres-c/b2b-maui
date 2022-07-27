from core.connectors.b2b.lapolar import LaPolarB2BConnector
from core.connectors.b2b.lapolar import LaPolarB2BFileConnector
from core.connectors.b2b.lapolar import LaPolarB2BStockFileConnector
from core.connectors.b2b.lapolar import LaPolarB2BOCConnector
from core.sources.b2b.base import B2BFileSource
from core.sources.b2b.base import B2BPortalSource
from core.sources.b2b.base import B2BWebSource


class LaPolarB2BPortalSource(B2BPortalSource):

    CONNECTOR = LaPolarB2BConnector

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class LaPolarB2BBaseSource(B2BFileSource):

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['repository_path'] = kwargs['repository_path']
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class LaPolarB2BFileSource(LaPolarB2BBaseSource):

    CONNECTOR = LaPolarB2BFileConnector


class LaPolarB2BStockSource(LaPolarB2BBaseSource):

    CONNECTOR = LaPolarB2BStockFileConnector


class LaPolarB2BOCWebSource(B2BWebSource):

    CONNECTOR = LaPolarB2BOCConnector

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args
