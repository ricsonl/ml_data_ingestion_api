import time
from typing import List
from pydantic import parse_obj_as
import pyarrow.parquet as pq
from pyarrow import Table
from pymongo.errors import DuplicateKeyError, CollectionInvalid, WriteError, OperationFailure
from pymongo.collection import ReturnDocument
from pymongo.database import Database
from schemas.raw import RawDataSchema
from serializers.raw import raw_data_list_entity

MAX_CHUNKSIZE = 10000

class RawDataService:
    @staticmethod
    def validate_collection_name(db: Database, collection_name: str):
        allowed_names = db.list_collection_names()
        q = '\''
        if collection_name not in allowed_names:
            raise CollectionInvalid(f"'collection_name' must be {' or '.join([f'{q}{an}{q}' for an in allowed_names])}")

    @staticmethod
    def create_data(db: Database, collection_name: str, data: RawDataSchema) -> None:
        if RawDataService.get_data(db, collection_name, data.ID_code):
            raise DuplicateKeyError(f"ID_code '{data.ID_code}' already exists in '{collection_name}' collection")
        else:
            db.get_collection(collection_name).insert_one(data.dict())


    @staticmethod
    def list_data(db: Database, collection_name: str, limit: int) -> List[ReturnDocument]:
        return raw_data_list_entity(db.get_collection(collection_name).find().limit(limit))
    

    @staticmethod
    def get_data(db: Database, collection_name: str, id_code: str) -> ReturnDocument:
        return raw_data_list_entity(db.get_collection(collection_name).find({'ID_code': id_code}))


    @staticmethod
    def update_data(db: Database, collection_name: str, data: RawDataSchema) -> None:
        if RawDataService.get_data(db, collection_name, data.ID_code):
            db.get_collection(collection_name).update_one({'ID_code': data.ID_code},  {"$set": data.dict()})
        else:
            raise WriteError(f"ID_code '{data.ID_code}' not found in '{collection_name}' collection")


    @staticmethod
    def delete_data(db: Database, collection_name: str, id_code: str) -> None:
        if RawDataService.get_data(db, collection_name, id_code):
            db.get_collection(collection_name).delete_one({'ID_code': id_code})
        else:
            raise OperationFailure(f"ID_code '{id_code}' not found in '{collection_name}' collection")


    @staticmethod
    def clear_collection(db: Database, collection_name: str) -> None:
        db.get_collection(collection_name).drop()
        db.create_collection(collection_name)


    @staticmethod
    def load_data(db: Database, collection_name: str, path: str) -> None:
        start_time = time.time()
        table: Table = pq.read_table(path)
        for i, batch in enumerate(table.to_batches(max_chunksize=MAX_CHUNKSIZE)):
            batch_dict = batch.to_pylist()
            parse_obj_as(List[RawDataSchema], batch_dict)
            db.get_collection(collection_name).insert_many(batch_dict, ordered=False)
            print(f'Batch {i+1} inserted ({len(batch_dict)} rows)')
        print(f'Done. Took {time.time() - start_time} seconds')