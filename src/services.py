from typing import List
from schemas import RawDataSchema
import pyarrow.parquet as pq
import time
from database import get_db
from pymongo.errors import DuplicateKeyError, CollectionInvalid, WriteError, OperationFailure
from serializers.raw_data_serializers import raw_data_list_entity
from pymongo.collection import ReturnDocument
from pydantic import parse_obj_as


class RawDataService:
    @staticmethod
    def validate_collection_name(collection_name: str):
        if collection_name not in get_db().list_collection_names():
            raise CollectionInvalid(f"There is no collection named '{collection_name}'")

    @staticmethod
    def create_data(collection_name: str, data: RawDataSchema) -> None:
        if RawDataService.get_data(collection_name, data.ID_code):
            raise DuplicateKeyError(f"ID_code '{data.ID_code}' already exists in '{collection_name}' collection")
        else:
            get_db().get_collection(collection_name).insert_one(data.dict())


    @staticmethod
    def list_data(collection_name: str, limit: int) -> List[ReturnDocument]:
        return raw_data_list_entity(get_db().get_collection(collection_name).find().limit(limit))
    

    @staticmethod
    def get_data(collection_name: str, id_code: str) -> ReturnDocument:
        return raw_data_list_entity(get_db().get_collection(collection_name).find({'ID_code': id_code}))


    @staticmethod
    def update_data(collection_name: str, data: RawDataSchema) -> None:
        if RawDataService.get_data(collection_name, data.ID_code):
            get_db().get_collection(collection_name).update_one({'ID_code': data.ID_code},  {"$set": data.dict()})
        else:
            raise WriteError(f"ID_code '{data.ID_code}' not found in '{collection_name}' collection")


    @staticmethod
    def delete_data(collection_name: str, id_code: str) -> None:
        if RawDataService.get_data(collection_name, id_code):
            get_db().get_collection(collection_name).delete_one({'ID_code': id_code})
        else:
            raise OperationFailure(f"ID_code '{id_code}' not found in '{collection_name}' collection")


    @staticmethod
    def clear_collection(collection_name: str) -> None:
        get_db().get_collection(collection_name).drop()
        get_db().create_collection(collection_name)


    @staticmethod
    def load_data(collection_name: str, path: str) -> None:
        start_time = time.time()
        table = pq.read_table(path)
        for i, batch in enumerate(table.to_batches(max_chunksize=100000)):
            batch_dict = batch.to_pylist()
            parse_obj_as(RawDataSchema, batch_dict)
            get_db().get_collection(collection_name).insert_many(batch_dict, ordered=False)
            print(f'Batch {i+1} inserted ({len(batch_dict)} rows)')
        print(f'Done. Took {time.time() - start_time} seconds')