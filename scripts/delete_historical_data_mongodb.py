from pymongo import MongoClient
import argparse

parser = argparse.ArgumentParser(description='Delete historical_data database from host')
parser.add_argument('host', type=str, help='Host. type1: mongodb://127.0.0.1:27017 ; type2: localhost')
args = parser.parse_args()


def delete(host):
    if host.startswith('mongodb://'):
        client = MongoClient(host)
    else:
        client = MongoClient(host, 27017)
    client.drop_database(client.historical_data)
    if "historical_data" in client.database_names():
        print("Error deleting 'historical_data' database")
    else:
        print("'historical_data' database deleted")


if __name__ == '__main__':
    delete(args.host)
