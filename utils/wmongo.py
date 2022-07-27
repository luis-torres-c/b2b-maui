import os

import pymongo


class MongoDbManager:

    ASCENDING = pymongo.ASCENDING
    DESCENDING = pymongo.DESCENDING

    @classmethod
    def connection(cls, database):
        host = os.environ['MONGODB_HOST']
        port = int(os.environ.get('MONGODB_PORT', 27017))
        return pymongo.MongoClient(host, port)[database]
