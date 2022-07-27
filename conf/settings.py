import os

PROD_ENV = bool(os.environ.get('WIVO_ENV'))
DASHBOARD_NAME = os.environ['DASHBOARD_NAME']
