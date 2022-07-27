from core.sources.b2b.base import B2BWebSource
from core.sources.b2b.base import B2BPortalSource, B2BFileSource
from core.connectors.b2b.ripley import RipleyB2BConnector, RipleyB2BFileConnector, RipleyB2BStockFileConnector, RipleyB2BOCConnector, RipleyB2BPeruConnector, RipleyB2BOCTestConnector


class RipleyB2BPortalSource(B2BPortalSource):

    CONNECTOR = RipleyB2BConnector

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class RipleyB2BBaseSource(B2BFileSource):

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['b2b_empresa'] = kwargs['b2b_empresa']
        args['repository_path'] = kwargs['repository_path']
        return args


class RipleyB2BFileSource(RipleyB2BBaseSource):

    CONNECTOR = RipleyB2BFileConnector


class RipleyB2BStockSource(RipleyB2BBaseSource):

    CONNECTOR = RipleyB2BStockFileConnector


class RipleyB2BOCWebSource(B2BWebSource):

    CONNECTOR = RipleyB2BOCConnector

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class RipleyB2BOCTestWebSource(B2BWebSource):

    CONNECTOR = RipleyB2BOCTestConnector

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class RipleyB2BPeruPortalSource(B2BPortalSource):

    CONNECTOR = RipleyB2BPeruConnector
