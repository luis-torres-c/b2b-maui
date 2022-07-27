from core.connectors.b2b.base import EmpresasSBB2BPortalConnector, EmpresasSBB2BSalesConnector, EmpresasSBB2BStockConnector


class PreunicB2BPortalConnector(EmpresasSBB2BPortalConnector):

    SOURCE_NAME = 'PREUNIC'


class PreunicB2BSalesConnector(EmpresasSBB2BSalesConnector):

    SOURCE_NAME = 'PREUNIC'


class PreunicB2BStockConnector(EmpresasSBB2BStockConnector):

    SOURCE_NAME = 'PREUNIC'
