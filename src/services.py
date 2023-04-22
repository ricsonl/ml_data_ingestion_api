from typing import List, Optional
from database.models import TrainData, TestData
from database.connection import async_session
from schemas import TrainDataSchema, TestDataSchema
from sqlalchemy import delete, update
from sqlalchemy.future import select

class TrainDataService:
    async def create_train_data(train_data: TrainDataSchema) -> None:
        async with async_session() as session:
            _train_data = TrainData(key=train_data.key,
                                    fare_amount=train_data.fare_amount,
                                    pickup_datetime=train_data.pickup_datetime,
                                    pickup_latitude=train_data.pickup_latitude,
                                    pickup_longitude=train_data.pickup_longitude,
                                    dropoff_latitude=train_data.dropoff_latitude,
                                    dropoff_longitude=train_data.dropoff_longitude,
                                    passenger_count=train_data.passenger_count)
            session.add(_train_data)
            await session.commit()
    

    async def list_train_data() -> Optional[List[TrainData]]:
        async with async_session() as session:
            result = await session.execute(select(TrainData))
            return result.scalars().all()
        

    async def get_train_data(key: str) -> Optional[TrainData]:
        async with async_session() as session:
            result = await session.execute(select(TrainData).where(TrainData.key==key))
            return result.scalars().first()
    

    async def update_train_data(train_data: TrainDataSchema) -> None:
        async with async_session() as session:
            await session.execute(update(TrainData).where(TrainData.key==train_data.key)
                                                   .values(key=train_data.key,
                                                           fare_amount=train_data.fare_amount,
                                                           pickup_datetime=train_data.pickup_datetime,
                                                           pickup_latitude=train_data.pickup_latitude,
                                                           pickup_longitude=train_data.pickup_longitude,
                                                           dropoff_latitude=train_data.dropoff_latitude,
                                                           dropoff_longitude=train_data.dropoff_longitude,
                                                           passenger_count=train_data.passenger_count))
            await session.commit()


    async def delete_train_data(key: str) -> None:
        async with async_session() as session:
            await session.execute(delete(TrainData).where(TrainData.key==key))
            await session.commit()


class TestDataService:
    async def create_test_data(test_data: TestDataSchema) -> None:
        async with async_session() as session:
            _test_data = TestData(key=test_data.key,
                                  pickup_datetime=test_data.pickup_datetime,
                                  pickup_latitude=test_data.pickup_latitude,
                                  pickup_longitude=test_data.pickup_longitude,
                                  dropoff_latitude=test_data.dropoff_latitude,
                                  dropoff_longitude=test_data.dropoff_longitude,
                                  passenger_count=test_data.passenger_count)
            session.add(_test_data)
            await session.commit()
    

    async def list_test_data() -> Optional[List[TestData]]:
        async with async_session() as session:
            result = await session.execute(select(TestData))
            return result.scalars().all()
        

    async def get_test_data(key: str) -> Optional[TestData]:
        async with async_session() as session:
            result = await session.execute(select(TestData).where(TestData.key==key))
            return result.scalars().first()
    

    async def update_test_data(test_data: TestDataSchema) -> None:
        async with async_session() as session:
            await session.execute(update(TestData).where(TestData.key==test_data.key)
                                                  .values(key=test_data.key,
                                                          pickup_datetime=test_data.pickup_datetime,
                                                          pickup_latitude=test_data.pickup_latitude,
                                                          pickup_longitude=test_data.pickup_longitude,
                                                          dropoff_latitude=test_data.dropoff_latitude,
                                                          dropoff_longitude=test_data.dropoff_longitude,
                                                          passenger_count=test_data.passenger_count))
            await session.commit()


    async def delete_test_data(key: str) -> None:
        async with async_session() as session:
            await session.execute(delete(TestData).where(TestData.key==key))
            await session.commit()