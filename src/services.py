from typing import List, Union, Optional, Any
from schemas import RawDataSchema
from sqlalchemy import delete, update
from sqlalchemy.future import select
import pandas as pd
import pyarrow.parquet as pq
import os
import time
# from datetime import datetime
# from fastapi import Depends, HTTPException, status, APIRouter, Response
# from pymongo.collection import ReturnDocument
# from app import schemas
from database import db
from serializers.raw_data_serializers import raw_data_entity, raw_data_list_entity
# from bson.objectid import ObjectId
# from pymongo.errors import DuplicateKeyError
from decimal import Decimal
from bson.decimal128 import Decimal128


def list_parquet_files(path: str) -> str:
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)) and file.endswith('.parquet'):
            yield file


def convert_decimal(dict):
    for i in range(0,len(dict.get('var'))):
        dict['var'][i] = Decimal128(str(dict.get('var')[i]))
    return dict


class RawDataService:
    def create_data(collection_name: str, data: RawDataSchema) -> None:
        db.get_collection(collection_name).insert_one(convert_decimal(data.dict()))


    def list_data(collection_name: str, limit: int) -> List[Any]:
        return raw_data_list_entity(db.get_collection(collection_name).find().limit(limit))
    

    def get_data(collection_name: str, id_code: str) -> Any:
        return raw_data_list_entity(db.get_collection(collection_name).find({'ID_code': id_code}))


    # async def list_data(table: Union[TrainRaw, TestRaw], limit: Optional[int] = None) -> List[Any]:
    #     async with async_session() as session:
    #         result = await session.execute(select(table).limit(limit))
    #         return result.scalars().all()
    
    # async def list_data(table: Union[TrainRaw, TestRaw], limit: Optional[int] = None) -> List[Any]:
    #     async with async_session() as session:
    #         result = await session.execute(select(table).limit(limit))
    #         return result.scalars().all()

    def update_data(collection_name: str, data: RawDataSchema) -> None:
        return db.get_collection(collection_name).replace_one({'ID_code': data.ID_code}, convert_decimal(data.dict()))


    # async def delete_data(table: Union[TrainRaw, TestRaw], key: str) -> None:
    #     async with async_session() as session:
    #         await session.execute(delete(table).where(table.key==key))
    #         await session.commit()


    # async def clear_data(table: Union[TrainRaw, TestRaw]) -> None:
    #     async with async_session() as session:
    #         await session.execute(delete(table))
    #         await session.commit()


    # async def load_data(table: Union[TrainRaw, TestRaw], path: str) -> None:
    #     async with async_session() as session:
    #         start_time = time.time()
    #         for filename in list_parquet_files(path):
    #             pq_part = pq.ParquetFile(os.path.join(path, filename))
    #             for record in pq_part.iter_batches(batch_size=50000):
    #                 dicts = record.to_pylist()
    #                 await session.execute(table.__table__.insert(), dicts)
    #         await session.commit()
    #         print(f'--- Took {time.time() - start_time} seconds ---')