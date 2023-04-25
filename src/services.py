from typing import List, Union, Optional, Any
from database.models import TrainRaw, TestRaw
from database.connection import async_session
from schemas import RawDataSchema
from sqlalchemy import delete, update
from sqlalchemy.future import select
import pandas as pd
import pyarrow.parquet as pq
import os
import time


def list_parquet_files(path: str) -> str:
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)) and file.endswith('.parquet'):
            yield file


def date_validate(str: str) -> Optional[str]:
    try:
        pd.to_datetime(str)
    except Exception as err:
        raise err
    return str


class RawDataService:
    def get_table(table_name):
        if table_name == 'train_raw':
            return TrainRaw
        elif table_name == 'test_raw':
            return TestRaw
        else:
            raise Exception("Raw data table name must be either 'train_raw' or 'test_raw'")


    async def create_data(table: Union[TrainRaw, TestRaw], data: RawDataSchema) -> None:
        _data = table(key=data.key,
                      fare_amount=data.fare_amount,
                      pickup_datetime=date_validate(data.pickup_datetime),
                      pickup_latitude=data.pickup_latitude,
                      pickup_longitude=data.pickup_longitude,
                      dropoff_latitude=data.dropoff_latitude,
                      dropoff_longitude=data.dropoff_longitude,
                      passenger_count=data.passenger_count)
        async with async_session() as session:
            session.add(_data)
            await session.commit()


    async def list_data(table: Union[TrainRaw, TestRaw], limit: Optional[int] = None) -> List[Any]:
        async with async_session() as session:
            result = await session.execute(select(table).limit(limit))
            return result.scalars().all()
        

    async def get_data(table: Union[TrainRaw, TestRaw], key: str) -> Any:
        async with async_session() as session:
            result = await session.execute(select(table).where(table.key==key))
            return result.scalars().first()
    

    async def update_data(table: Union[TrainRaw, TestRaw], data: RawDataSchema) -> None:
        async with async_session() as session:
            await session.execute(update(table).where(table.key==data.key)
                                                    .values(key=data.key,
                                                            fare_amount=data.fare_amount,
                                                            pickup_datetime=date_validate(data.pickup_datetime),
                                                            pickup_latitude=data.pickup_latitude,
                                                            pickup_longitude=data.pickup_longitude,
                                                            dropoff_latitude=data.dropoff_latitude,
                                                            dropoff_longitude=data.dropoff_longitude,
                                                            passenger_count=data.passenger_count))           
            await session.commit()


    async def delete_data(table: Union[TrainRaw, TestRaw], key: str) -> None:
        async with async_session() as session:
            await session.execute(delete(table).where(table.key==key))
            await session.commit()


    async def clear_data(table: Union[TrainRaw, TestRaw]) -> None:
        async with async_session() as session:
            await session.execute(delete(table))
            await session.commit()


    async def load_data(table: Union[TrainRaw, TestRaw], path: str) -> None:
        async with async_session() as session:
            start_time = time.time()
            for filename in list_parquet_files(path):
                pq_part = pq.ParquetFile(os.path.join(path, filename))
                for record in pq_part.iter_batches(batch_size=50000):
                    dicts = record.to_pylist()
                    await session.execute(table.__table__.insert(), dicts)
            await session.commit()
            print(f'--- Took {time.time() - start_time} seconds ---')