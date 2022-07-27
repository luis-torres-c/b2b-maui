from core.sources.b2b.base import B2BWebSource
from core.connectors.b2b.pcfactory import PcFactoryB2BConnector


class PcFactoryB2BWebSource(B2BWebSource):

    CONNECTOR = PcFactoryB2BConnector

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['captcha_key'] = kwargs['captcha_key']
        args['captcha_googlekey'] = kwargs['captcha_googlekey']
        return args
