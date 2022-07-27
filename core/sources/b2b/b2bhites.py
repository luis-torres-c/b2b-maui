from core.connectors.b2b.hites import HitesB2BConnector
from core.connectors.b2b.hites import HitesB2BFileConnector
from core.connectors.b2b.hites import HitesB2BStockFileConnector
from core.connectors.b2b.hites import HitesB2BOCConnector
from core.sources.b2b.base import B2BFileSource
from core.sources.b2b.base import B2BPortalSource
from core.sources.b2b.base import B2BWebSource


class HitesB2BPortalSource(B2BPortalSource):

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['b2b_empresa'] = kwargs.get('b2b_empresa', '')
        return args

    CONNECTOR = HitesB2BConnector


class HitesB2BBaseSource(B2BFileSource):

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['repository_path'] = kwargs['repository_path']
        args['b2b_empresa'] = kwargs['b2b_empresa']
        return args


class HitesB2BFileSource(HitesB2BBaseSource):

    CONNECTOR = HitesB2BFileConnector


class HitesB2BStockSource(HitesB2BBaseSource):

    CONNECTOR = HitesB2BStockFileConnector


class HitesB2BOCWebSource(B2BWebSource):

    CONNECTOR = HitesB2BOCConnector
