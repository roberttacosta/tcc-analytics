from pymongo import MongoClient

MONGO_URI = 'mongodb://localhost:27017/'
DATABASE_NAME = 'defi_data'
COLLECTION_NAME = 'transactions'


def get_mongo_client():
    client = MongoClient(MONGO_URI)
    return client[DATABASE_NAME][COLLECTION_NAME]
