from core.sources.b2b.base import B2BFileSource
from core.sources.b2b.base import B2BPortalSource
from core.connectors.b2b.abcdin import AbcdinB2BConnector
from core.connectors.b2b.abcdin import AbcdinB2BFileConnector
from core.connectors.b2b.abcdin import AbcdinB2BStockFileConnector


class AbcdinB2BPortalSource(B2BPortalSource):

    CONNECTOR = AbcdinB2BConnector

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class AbcdinB2BBaseSource(B2BFileSource):

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['repository_path'] = kwargs['repository_path']
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class AbcdinB2BFileSource(AbcdinB2BBaseSource):

    CONNECTOR = AbcdinB2BFileConnector


class AbcdinB2BStockSource(AbcdinB2BBaseSource):

    CONNECTOR = AbcdinB2BStockFileConnector
