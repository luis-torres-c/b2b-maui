from core.connectors.rocketpin import RocketpinConnector
from core.sources.base import Source

from utils.logger import logger


class RocketpinSource(Source):
    CONNECTOR = RocketpinConnector

    def parse_data(self, connector, **kwargs):
        raise NotImplementedError

    def process(self, **kwargs):
        conn = self.CONNECTOR.get_instance(**kwargs)
        logger.info('Starting data parsing...')
        return self.parse_data(conn, **kwargs)
