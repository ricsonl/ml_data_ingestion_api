import os
from pymongo.mongo_client import MongoClient
import pymongo
from dotenv import load_dotenv
load_dotenv()

class Database:
    instance = None
    def __new__(cls):
        if cls.instance is None:
            cls.instance = super().__new__(cls)
            cls.instance.client = MongoClient(os.getenv('DATABASE_URL'))
            cls.instance.db = cls.instance.client[os.getenv('MONGO_INITDB_DATABASE')]
            TrainRaw = cls.instance.db.train_raw
            TestRaw = cls.instance.db.test_raw
            TrainRaw.create_index([("ID_code", pymongo.ASCENDING)], unique=True)
            TestRaw.create_index([("ID_code", pymongo.ASCENDING)], unique=True)
        return cls.instance

def get_db() -> Database:
    return Database().db