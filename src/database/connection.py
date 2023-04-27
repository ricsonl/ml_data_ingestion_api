import os
from typing import Any
from pymongo.mongo_client import MongoClient
from pymongo.database import Database
import pymongo
from dotenv import load_dotenv

load_dotenv()

class MongoManager:
    class __MongoManager:
        client: MongoClient[Any] = MongoClient(os.getenv('DATABASE_URL'))
        db: Database[Any] = client[os.getenv('MONGO_INITDB_DATABASE') or '']

    __instance = None

    def __init__(self) -> None:
        if not MongoManager.__instance:
            MongoManager.__instance = MongoManager.__MongoManager()
            TrainRaw = getattr(MongoManager.__instance.db, 'train_raw')
            TestRaw = getattr(MongoManager.__instance.db, 'test_raw')
            TrainRaw.create_index([("ID_code", pymongo.ASCENDING)], unique=True)
            TestRaw.create_index([("ID_code", pymongo.ASCENDING)], unique=True)

    def __getattr__(self, item: str) -> Any:
        return getattr(self.__instance, item)