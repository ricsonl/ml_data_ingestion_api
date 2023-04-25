from typing import List, Any
from schemas import RawDataSchema
import pyarrow.parquet as pq
import os
import time
from database import db
from pymongo.errors import DuplicateKeyError, CollectionInvalid, WriteError, OperationFailure
from serializers.raw_data_serializers import raw_data_list_entity
from utils import list_parquet_files, convert_vars_decimal
from pymongo.collection import ReturnDocument


class RawDataService:
    @staticmethod
    def validate_collection_name(collection_name: str):
        if collection_name not in db.list_collection_names():
            raise CollectionInvalid(f"There is no collection named '{collection_name}'")

    @staticmethod
    def create_data(collection_name: str, data: RawDataSchema) -> None:
        if RawDataService.get_data(collection_name, data.ID_code):
            raise DuplicateKeyError(f"ID_code '{data.ID_code}' already exists in '{collection_name}' collection")
        else:
            db.get_collection(collection_name).insert_one(convert_vars_decimal(data.dict()))


    @staticmethod
    def list_data(collection_name: str, limit: int) -> List[ReturnDocument]:
        return raw_data_list_entity(db.get_collection(collection_name).find().limit(limit))
    

    @staticmethod
    def get_data(collection_name: str, id_code: str) -> ReturnDocument:
        return raw_data_list_entity(db.get_collection(collection_name).find({'ID_code': id_code}))


    @staticmethod
    def update_data(collection_name: str, data: RawDataSchema) -> None:
        if RawDataService.get_data(collection_name, data.ID_code):
            db.get_collection(collection_name).update_one({'ID_code': data.ID_code},  {"$set": convert_vars_decimal(data.dict())})
        else:
            raise WriteError(f"ID_code '{data.ID_code}' not found in '{collection_name}' collection")


    @staticmethod
    def delete_data(collection_name: str, id_code: str) -> None:
        if RawDataService.get_data(collection_name, id_code):
            db.get_collection(collection_name).delete_one({'ID_code': id_code})
        else:
            raise OperationFailure(f"ID_code '{id_code}' not found in '{collection_name}' collection")


    @staticmethod
    def clear_collection(collection_name: str) -> None:
        db.get_collection(collection_name).delete_many({})


    @staticmethod
    def load_data(collection_name: str, path: str) -> None:
        start_time = time.time()
        i = 0
        for filename in list_parquet_files(path):
            pq_part = pq.ParquetFile(os.path.join(path, filename))
            for record in pq_part.iter_batches(batch_size=100000):
                data = record.to_pylist()
                i += len(data)
                db.get_collection(collection_name).insert_many(record.to_pylist())
        print(f'--- Took {time.time() - start_time} seconds to insert {i} rows ---')