from core.sources.b2b.base import B2BPortalSource, B2BFileSource
from core.connectors.b2b.preunic import PreunicB2BPortalConnector, PreunicB2BSalesConnector, PreunicB2BStockConnector


class PreunicB2BPortalSource(B2BPortalSource):

    CONNECTOR = PreunicB2BPortalConnector


class PreunicB2BBaseSource(B2BFileSource):

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['repository_path'] = kwargs['repository_path']
        return args


class PreunicB2BSalesSource(PreunicB2BBaseSource):

    CONNECTOR = PreunicB2BSalesConnector


class PreunicB2BStockSource(PreunicB2BBaseSource):

    CONNECTOR = PreunicB2BStockConnector
