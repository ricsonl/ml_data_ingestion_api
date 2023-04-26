import os
from pymongo.mongo_client import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
import pymongo
from dotenv import load_dotenv

load_dotenv()

class MongoManager:
    instance = None
    def __new__(cls):
        if cls.instance is None:
            cls.instance: MongoManager = super().__new__(cls)
            cls.instance.client: MongoClient = MongoClient(os.getenv('DATABASE_URL'))
            cls.instance.db: Database = cls.instance.client[os.getenv('MONGO_INITDB_DATABASE')]
            TrainRaw: Collection = cls.instance.db.train_raw
            TestRaw: Collection = cls.instance.db.test_raw
            TrainRaw.create_index([("ID_code", pymongo.ASCENDING)], unique=True)
            TestRaw.create_index([("ID_code", pymongo.ASCENDING)], unique=True)
        return cls.instance

def get_instance() -> MongoManager:
    return MongoManager()