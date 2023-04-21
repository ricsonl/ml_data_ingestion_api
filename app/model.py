from sqlalchemy import Column, Integer, DECIMAL, String
from config import Base

class TrainData(Base):
    __tablename__ = 'train_data'

    key=Column(String, primary_key=True)
    fare_amount=Column(DECIMAL(19,4))
    pickup_datetime=Column(String)
    pickup_latitude=Column(DECIMAL(8,6))
    pickup_longitude=Column(DECIMAL(9,6))
    dropoff_latitude=Column(DECIMAL(8,6))
    dropoff_longitude=Column(DECIMAL(9,6))
    passenger_count=Column(Integer)

class TestData(Base):
    __tablename__ = 'test_data'

    key=Column(String, primary_key=True)
    pickup_datetime=Column(String)
    pickup_latitude=Column(DECIMAL(8,6))
    pickup_longitude=Column(DECIMAL(9,6))
    dropoff_latitude=Column(DECIMAL(8,6))
    dropoff_longitude=Column(DECIMAL(9,6))
    passenger_count=Column(Integer)