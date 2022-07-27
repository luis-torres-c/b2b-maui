import requests
import xmltodict

from core.connectors import Connector
from utils.logger import logger


class OpenbravoConnector(Connector):

    PARAM_FILTER = "?where=orderDate>=%27{}T00:00:00.0Z%27%20and%20"
    PARAM_FILTER += "orderDate<=%27{}T23:59:59.0Z%27&_selectedProperties=id,orderLineList,client,organization,createdBy,businessPartner,obposApplications,documentNo,warehouse,orderLineList.id,orderLineList.creationDate,orderLineList.product,orderLineList.orderedQuantity,orderLineList.lineNetAmount,orderLineList.orderDate,obloyMembershipid"

    @classmethod
    def get_instance(cls, **kwargs):
        user = kwargs['openbravo_user']
        password = kwargs['openbravo_password']
        return cls(user, password)

    def __init__(self, user, password):
        self.user = user
        self.password = password

    def execute_request(self, url, string_date):
        param = self.PARAM_FILTER.format(string_date, string_date)
        logger.debug('Making Request to {}'.format(url + param))
        resp = requests.get(url + param, auth=(self.user, self.password), timeout=500)
        if resp.status_code != requests.codes.ok:
            logger.error("Error {} al servidor".format(resp.status_code))
            return ()
        d = xmltodict.parse(resp.content)
        return d

    def get_data(self, url):
        resp = requests.get(url, auth=(self.user, self.password), timeout=500)
        if resp.status_code != requests.codes.ok:
            return ()
        d = xmltodict.parse(resp.content)
        return d
