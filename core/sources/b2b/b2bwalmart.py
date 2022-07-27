from core.sources.b2b.base import B2BPortalSource, B2BFileSource
from core.connectors.b2b.walmart import WalmartB2BConnector, WalmartB2BFileConnector, WalmartB2BFileStockConnector


class WalmartB2BPortalSource(B2BPortalSource):

    CONNECTOR = WalmartB2BConnector


class WalmartB2BFileBaseSource(B2BFileSource):

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['repository_path'] = kwargs['repository_path']
        return args


class WalmartB2BFileSource(WalmartB2BFileBaseSource):

    CONNECTOR = WalmartB2BFileConnector


class WalmartB2BFileStockSource(WalmartB2BFileBaseSource):

    CONNECTOR = WalmartB2BFileStockConnector
