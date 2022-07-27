from core.connectors.sftp import SFTPConnector
from core.sources import FileSource


class SFTPSource(FileSource):

    CONNECTOR = SFTPConnector
