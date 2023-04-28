import time
from typing import Optional, List, Any, Dict
from pydantic import parse_obj_as
import pyarrow.parquet as pq
from pyarrow import Table
from pymongo.errors import DuplicateKeyError, WriteError, OperationFailure
from pymongo.typings import _DocumentType
from pymongo.database import Database
from services.base import BaseService
from schemas.raw import RawDataSchema
from serializers.raw import raw_data_list_entity

MAX_CHUNKSIZE = 50000

class RawDataService(BaseService):
    @staticmethod
    def create_data(db: Database[_DocumentType], collection_name: str, data: RawDataSchema) -> None:
        if RawDataService.get_data(db, collection_name, data.ID_code):
            raise DuplicateKeyError(f"ID_code '{data.ID_code}' already exists in '{collection_name}' collection")
        else:
            db.get_collection(collection_name).insert_one(data.dict(exclude_unset=True)) # type: ignore


    @staticmethod
    def list_data(db: Database[_DocumentType], collection_name: str, limit: int) -> List[Dict[str, Any]]:
        return raw_data_list_entity(db.get_collection(collection_name).find().limit(limit))
    

    @staticmethod
    def get_data(db: Database[_DocumentType], collection_name: str, id_code: Optional[str]) -> List[Dict[str, Any]]:
        return raw_data_list_entity(db.get_collection(collection_name).find({'ID_code': id_code}))


    @staticmethod
    def update_data(db: Database[_DocumentType], collection_name: str, data: RawDataSchema) -> None:
        if RawDataService.get_data(db, collection_name, data.ID_code):
            db.get_collection(collection_name).update_one({'ID_code': data.ID_code},  {"$set": data.dict(exclude_unset=True)})
        else:
            raise WriteError(f"ID_code '{data.ID_code}' not found in '{collection_name}' collection")


    @staticmethod
    def delete_data(db: Database[_DocumentType], collection_name: str, id_code: str) -> None:
        if RawDataService.get_data(db, collection_name, id_code):
            db.get_collection(collection_name).delete_one({'ID_code': id_code})
        else:
            raise OperationFailure(f"ID_code '{id_code}' not found in '{collection_name}' collection")


    @staticmethod
    def clear_collection(db: Database[_DocumentType], collection_name: str) -> None:
        db.get_collection(collection_name).drop()
        db.create_collection(collection_name)


    @staticmethod
    def load_data(db: Database[_DocumentType], collection_name: str, path: str) -> None:
        start_time = time.time()
        table: Table = pq.read_table(path)
        for i, batch in enumerate(table.to_batches(max_chunksize=MAX_CHUNKSIZE)):
            batch_dict = batch.to_pylist() # type: ignore
            parse_obj_as(List[RawDataSchema], batch_dict)
            db.get_collection(collection_name).insert_many(batch_dict, ordered=False)
            print(f'Batch {i+1} inserted ({len(batch_dict)} rows)')
        print(f'Done. Took {time.time() - start_time} seconds')