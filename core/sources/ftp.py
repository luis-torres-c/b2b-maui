from core.connectors.ftp import FTPConnector
from core.sources import FileSource


class FTPSource(FileSource):

    CONNECTOR = FTPConnector
