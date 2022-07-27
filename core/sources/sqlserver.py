from core.connectors.sqlserver import SQLServerConnector
from core.sources.base import DataBaseSource


class SQLServerSource(DataBaseSource):
    CONNECTOR = SQLServerConnector
