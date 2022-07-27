from core.sources.b2b.base import B2BFileSource
from core.sources.b2b.base import B2BPortalSource
from core.connectors.b2b.jumbo import NewJumboB2BConnector
from core.connectors.b2b.jumbo import NewJumboB2BFileConnector
from core.connectors.b2b.jumbo import NewJumboB2BStockFileConnector


class NewJumboB2BPortalSource(B2BPortalSource):

    CONNECTOR = NewJumboB2BConnector

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class NewJumboB2BBaseSource(B2BFileSource):

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['repository_path'] = kwargs['repository_path']
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class NewJumboB2BFileSource(NewJumboB2BBaseSource):

    CONNECTOR = NewJumboB2BFileConnector


class NewJumboB2BStockSource(NewJumboB2BBaseSource):

    CONNECTOR = NewJumboB2BStockFileConnector
