import pymssql

from core.connectors import Connector


class SQLServerConnector(Connector):

    @classmethod
    def get_instance(cls, **kwargs):
        host = kwargs['sqlserver_host']
        user = kwargs['sqlserver_user']
        password = kwargs['sqlserver_password']
        return cls(host, user, password)

    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password

    def execute_query(self, query, params=(), database=''):
        conn = pymssql.connect(self.host, self.user, self.password, database)
        cursor = conn.cursor(as_dict=True)
        cursor.execute(query, params)
        return cursor
