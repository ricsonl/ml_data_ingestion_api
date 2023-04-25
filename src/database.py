import os
from pymongo import mongo_client
import pymongo
from dotenv import load_dotenv
load_dotenv()

client = mongo_client.MongoClient(os.getenv('DATABASE_URL'))

db = client[os.getenv('MONGO_INITDB_DATABASE')]
TrainRaw = db.train_raw
TestRaw = db.test_raw
TrainRaw.create_index([("ID_code", pymongo.ASCENDING)], unique=True)
TestRaw.create_index([("ID_code", pymongo.ASCENDING)], unique=True)