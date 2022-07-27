from core.sources.b2b.base import B2BFileSource
from core.sources.b2b.base import B2BPortalSource
from core.sources.b2b.base import B2BWebSource
from core.connectors.b2b.paris import ParisB2BConnector
from core.connectors.b2b.paris import ParisB2BFileConnector
from core.connectors.b2b.paris import ParisB2BOCConnector
from core.connectors.b2b.paris import ParisB2BStockFileConnector


class ParisB2BPortalSource(B2BPortalSource):

    CONNECTOR = ParisB2BConnector

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class ParisB2BBaseSource(B2BFileSource):

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['repository_path'] = kwargs['repository_path']
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class ParisB2BFileSource(ParisB2BBaseSource):

    CONNECTOR = ParisB2BFileConnector


class ParisB2BStockSource(ParisB2BBaseSource):

    CONNECTOR = ParisB2BStockFileConnector


class ParisB2BOCWebSource(B2BWebSource):

    CONNECTOR = ParisB2BOCConnector

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args
