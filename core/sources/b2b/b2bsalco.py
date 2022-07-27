from core.sources.b2b.base import B2BPortalSource, B2BFileSource
from core.connectors.b2b.salco import SalcoB2BPortalConnector, SalcoB2BSalesConnector, SalcoB2BStockConnector


class SalcoB2BPortalSource(B2BPortalSource):

    CONNECTOR = SalcoB2BPortalConnector


class SalcoB2BBaseSource(B2BFileSource):

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['repository_path'] = kwargs['repository_path']
        return args


class SalcoB2BSalesSource(SalcoB2BBaseSource):

    CONNECTOR = SalcoB2BSalesConnector


class SalcoB2BStockSource(SalcoB2BBaseSource):

    CONNECTOR = SalcoB2BStockConnector
