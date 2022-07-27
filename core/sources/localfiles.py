from core.sources.base import FileSource
from core.connectors.localfilesystem import LocalFileSystemConnector


class LocalFilesSource(FileSource):
    CONNECTOR = LocalFileSystemConnector
