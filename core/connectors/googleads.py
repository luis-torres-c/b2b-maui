from googleads import adwords
from googleads import oauth2

from core.connectors.base import Connector


class GoogleAdsConnector(Connector):
    USER_AGENT = 'wivo'

    @classmethod
    def get_instance(cls, **kwargs):
        client_id = kwargs['client_id']
        client_secret = kwargs['client_secret']
        refresh_token = kwargs['refresh_token']
        developer_token = kwargs['developer_token']
        client_customer_id = kwargs['client_customer_id']
        return cls(client_id, client_secret, refresh_token, developer_token, client_customer_id)

    def __init__(self, client_id, client_secret, refresh_token, developer_token, client_customer_id):
        self.oauth2_client = oauth2.GoogleRefreshTokenClient(client_id, client_secret, refresh_token)
        self.ad_words_client = adwords.AdWordsClient(
            developer_token, self.oauth2_client, self.USER_AGENT, client_customer_id=client_customer_id
        )

    def service(self, service_name):
        return self.ad_words_client.GetService(service_name)
