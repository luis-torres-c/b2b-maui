from core.connectors.b2b.falabella import FalabellaB2BPortalConnector
from core.connectors.b2b.falabella import FalabellaB2BFileConnector
from core.connectors.b2b.falabella import FalabellaB2BStockConnector


class TottusB2BPortalConnector(FalabellaB2BPortalConnector):
    BASE_URL = 'https://b2b.tottus.com/b2btoclpr/'

    CADENA = "8"
    DATE_STYLE = "width:9%;text-align:center"
    PORTAL = 'Tottus'

    WEEK_DAYS_FOR_DAILY_CASE = [
        'LUNES',
        'MARTES',
        'MIERCOLE',
        'JUEVES',
        'VIERNES',
        'SABADO',
        'DOMINGO'
    ]

    WEEK_DAYS_IN_WEEKLY_CASE = [
        'LUNES',
        'MARTES',
        'MIERCOLES',
        'JUEVES',
        'VIERNES',
        'SABADO',
        'DOMINGO'
    ]


class TottusB2BFileConnector(FalabellaB2BFileConnector):

    fixed_sub_folder = 'Tottus'

    WEEK_DAYS_FOR_DAILY_CASE = [
        'LUNES',
        'MARTES',
        'MIERCOLE',
        'JUEVES',
        'VIERNES',
        'SABADO',
        'DOMINGO'
    ]

    WEEK_DAYS_IN_WEEKLY_CASE = [
        'LUNES',
        'MARTES',
        'MIERCOLES',
        'JUEVES',
        'VIERNES',
        'SABADO',
        'DOMINGO'
    ]


class TottusB2BStockConnector(FalabellaB2BStockConnector):

    fixed_sub_folder = 'Tottus'
