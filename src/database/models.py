from sqlalchemy import Column, Integer, NUMERIC, String, TIMESTAMP
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class TrainRaw(Base):
    __tablename__ = 'train_raw'

    key=Column(String, primary_key=True, autoincrement=False)
    fare_amount=Column(NUMERIC)
    pickup_datetime=Column(TIMESTAMP)
    pickup_latitude=Column(NUMERIC)
    pickup_longitude=Column(NUMERIC)
    dropoff_latitude=Column(NUMERIC)
    dropoff_longitude=Column(NUMERIC)
    passenger_count=Column(Integer)

class TestRaw(Base):
    __tablename__ = 'test_raw'

    key=Column(String, primary_key=True, autoincrement=False)
    fare_amount=Column(NUMERIC)
    pickup_datetime=Column(TIMESTAMP)
    pickup_latitude=Column(NUMERIC)
    pickup_longitude=Column(NUMERIC)
    dropoff_latitude=Column(NUMERIC)
    dropoff_longitude=Column(NUMERIC)
    passenger_count=Column(Integer)