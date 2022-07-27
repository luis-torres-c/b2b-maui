from core.sources.b2b.base import B2BPortalSource, B2BFileSource
from core.connectors.b2b.maicao import MaicaoB2BConnector, MaicaoB2BFileConnector


class MaicaoB2BPortalSource(B2BPortalSource):

    CONNECTOR = MaicaoB2BConnector


class MaicaoB2BFileSource(B2BFileSource):

    CONNECTOR = MaicaoB2BFileConnector

    def set_arguments(self, **kwargs):
        args = super().set_arguments(**kwargs)
        args['repository_path'] = kwargs['repository_path']
        return args
