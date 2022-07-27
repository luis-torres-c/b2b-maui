from core.sources.base import FileSource
from core.connectors.excelfiles import ExcelFilesConnector


class ExcelFilesSource(FileSource):
    CONNECTOR = ExcelFilesConnector
