from sentry_sdk import capture_exception
from sentry_sdk import Hub
from sentry_sdk import init
from sentry_sdk import configure_scope

from conf.settings import PROD_ENV, DASHBOARD_NAME
from utils.logger import logger

from paramiko.ssh_exception import SSHException


_sentry_dns = 'https://09f24b6bd95b4bbeafb0fd284409317d@sentry.io/1800730'


class _SentryIO:
    @classmethod
    def initialize_sentryio(cls):
        init(dsn=_sentry_dns, max_breadcrumbs=50, ignore_errors=[SSHException])
        with configure_scope() as scope:
            scope.set_tag('dashboard_name', DASHBOARD_NAME)
        return cls

    @classmethod
    def catch_exception(cls, exc):
        capture_exception(exc)

    @classmethod
    def clean_gracefully(cls):
        client = Hub.current.client
        if client is not None:
            client.close(timeout=3.0)


class _DummySentryIO:
    @classmethod
    def initialize_sentryio(cls):
        return cls

    @classmethod
    def catch_exception(cls, exc):
        pass

    @classmethod
    def clean_gracefully(cls):
        pass


if PROD_ENV:
    logger.info('SentryIO is active')

_sentry_class = _SentryIO if PROD_ENV else _DummySentryIO
sentryio = _sentry_class.initialize_sentryio()
