from core.sources.b2b.base import B2BFileSource
from core.sources.b2b.base import B2BWebSource
from core.sources.b2b.base import B2BPortalSource
from core.connectors.b2b.corona import CoronaB2BConnector
from core.connectors.b2b.corona import CoronaB2BFileConnector
from core.connectors.b2b.corona import CoronaB2BOCConnector
from core.connectors.b2b.corona import CoronaB2BStockFileConnector


class CoronaB2BPortalSource(B2BPortalSource):

    CONNECTOR = CoronaB2BConnector

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class CoronaB2BBaseSource(B2BFileSource):

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['repository_path'] = kwargs['repository_path']
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class CoronaB2BFileSource(CoronaB2BBaseSource):

    CONNECTOR = CoronaB2BFileConnector


class CoronaB2BStockSource(CoronaB2BBaseSource):

    CONNECTOR = CoronaB2BStockFileConnector


class CoronaB2BOCWebSource(B2BWebSource):

    CONNECTOR = CoronaB2BOCConnector
