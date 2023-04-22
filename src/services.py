from typing import List, Optional, Union
from database.models import TrainData, TestData
from database.connection import async_session
from schemas import RawDataSchema
from sqlalchemy import delete, update
from sqlalchemy.future import select

class RawDataService:
    async def create_data(table: str, data: RawDataSchema) -> None:
        async with async_session() as session:
            if table == 'train':
                _data = TrainData(key=data.key,
                                fare_amount=data.fare_amount,
                                pickup_datetime=data.pickup_datetime.replace(tzinfo=None),
                                pickup_latitude=data.pickup_latitude,
                                pickup_longitude=data.pickup_longitude,
                                dropoff_latitude=data.dropoff_latitude,
                                dropoff_longitude=data.dropoff_longitude,
                                passenger_count=data.passenger_count)
            elif table == 'test':
                _data = TestData(key=data.key,
                                pickup_datetime=data.pickup_datetime.replace(tzinfo=None),
                                pickup_latitude=data.pickup_latitude,
                                pickup_longitude=data.pickup_longitude,
                                dropoff_latitude=data.dropoff_latitude,
                                dropoff_longitude=data.dropoff_longitude,
                                passenger_count=data.passenger_count)
            else:
                raise Exception("Raw data table must be 'train' or 'test'")
            session.add(_data)
            await session.commit()

    async def list_data(table: str) -> Union[Optional[List[TrainData]], Optional[List[TestData]]]:
        async with async_session() as session:
            if table == 'train':
                result = await session.execute(select(TrainData))
            elif table == 'test':
                result = await session.execute(select(TestData))
            else:
                raise Exception("Raw data table must be 'train' or 'test'")
            return result.scalars().all()
        

    async def get_data(table: str, key: str) -> Union[Optional[TrainData], Optional[TestData]]:
        async with async_session() as session:
            if table == 'train':
                result = await session.execute(select(TrainData).where(TrainData.key==key))
            elif table == 'test':
                result = await session.execute(select(TestData).where(TestData.key==key))
            else:
                raise Exception("Raw data table must be 'train' or 'test'")
            return result.scalars().first()
    

    async def update_data(table: str, data: RawDataSchema) -> None:
        async with async_session() as session:
            if table == 'train':
                await session.execute(update(TrainData).where(TrainData.key==data.key)
                                                        .values(key=data.key,
                                                                fare_amount=data.fare_amount,
                                                                pickup_datetime=data.pickup_datetime.replace(tzinfo=None),
                                                                pickup_latitude=data.pickup_latitude,
                                                                pickup_longitude=data.pickup_longitude,
                                                                dropoff_latitude=data.dropoff_latitude,
                                                                dropoff_longitude=data.dropoff_longitude,
                                                                passenger_count=data.passenger_count))
            elif table == 'test':
                await session.execute(update(TestData).where(TestData.key==data.key)
                                                        .values(key=data.key,
                                                                pickup_datetime=data.pickup_datetime.replace(tzinfo=None),
                                                                pickup_latitude=data.pickup_latitude,
                                                                pickup_longitude=data.pickup_longitude,
                                                                dropoff_latitude=data.dropoff_latitude,
                                                                dropoff_longitude=data.dropoff_longitude,
                                                                passenger_count=data.passenger_count))
            else:
                raise Exception("Raw data table must be 'train' or 'test'")            
            await session.commit()


    async def delete_data(table: str, key: str) -> None:
        async with async_session() as session:
            if table == 'train':
                await session.execute(delete(TrainData).where(TrainData.key==key))
            elif table == 'test':
                await session.execute(delete(TestData).where(TestData.key==key))
            else:
                raise Exception("Raw data table must be 'train' or 'test'")
            await session.commit()