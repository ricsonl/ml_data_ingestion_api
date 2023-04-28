import pandas as pd
from pymongo.typings import _DocumentType
from pymongo.database import Database
from services.base import BaseService

class StatsService(BaseService):
    @staticmethod
    def update_data(db: Database[_DocumentType], collection_name: str) -> None:
        pass
        # cursor = db.get_collection(collection_name).find({})
        # df = pd.DataFrame(list(cursor))
        # print(df.head())
        # print(df.describe())