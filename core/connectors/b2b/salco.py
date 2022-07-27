from core.connectors.b2b.base import EmpresasSBB2BPortalConnector, EmpresasSBB2BSalesConnector, EmpresasSBB2BStockConnector


class SalcoB2BPortalConnector(EmpresasSBB2BPortalConnector):

    SOURCE_NAME = 'SALCOBRAND'


class SalcoB2BSalesConnector(EmpresasSBB2BSalesConnector):

    SOURCE_NAME = 'SALCOBRAND'


class SalcoB2BStockConnector(EmpresasSBB2BStockConnector):

    SOURCE_NAME = 'SALCOBRAND'
