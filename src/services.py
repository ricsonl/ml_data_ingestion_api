from typing import List, Union, Any
from database.models import TrainRaw, TestRaw
from database.connection import async_session
from schemas import RawDataSchema
from sqlalchemy import delete, update
from sqlalchemy.future import select
import pandas as pd

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
                      pickup_datetime=pd.to_datetime(data.pickup_datetime).replace(tzinfo=None),
                      pickup_latitude=data.pickup_latitude,
                      pickup_longitude=data.pickup_longitude,
                      dropoff_latitude=data.dropoff_latitude,
                      dropoff_longitude=data.dropoff_longitude,
                      passenger_count=data.passenger_count)
        async with async_session() as session:
            session.add(_data)
            await session.commit()


    async def list_data(table: Union[TrainRaw, TestRaw]) -> List[Any]:
        async with async_session() as session:
            result = await session.execute(select(table))
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
                                                            pickup_datetime=pd.to_datetime(data.pickup_datetime).replace(tzinfo=None),
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
        _df = pd.read_parquet(path)
        _df['pickup_datetime'] =  pd.to_datetime(_df['pickup_datetime'])
        _df['pickup_datetime'] =  _df['pickup_datetime'].dt.tz_localize(None)
        _dict = _df.to_dict(orient='records')
        async with async_session() as session:
            buffer = []
            for row in _dict:
                buffer.append(row)
                if len(buffer) % 10000 == 0:
                    await session.execute(table.__table__.insert(), buffer)
                    buffer = []
            await session.execute(table.__table__.insert(), buffer)
            await session.commit()