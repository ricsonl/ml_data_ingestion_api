from typing import List, Optional
from decimal import Decimal
from sqlalchemy.orm import Session
from model import TrainData, TestData
from schemas import TrainDataSchema, TestDataSchema


# --- Taxifare Train Data ---

def get_train_data(db: Session, skip: int=0, limit: int=100) -> List[TrainData]:
    return db.query(TrainData).offset(skip).limit(limit).all()


def get_train_data_by_key(db: Session, key: str) -> Optional[TrainData]:
    return db.query(TrainData).filter(TrainData.key == key).first()


def create_train_data(db: Session, train_data: TrainDataSchema) -> TrainData:
    _train_data = TrainData(key=train_data.key,
                            fare_amount=train_data.fare_amount,
                            pickup_datetime=train_data.pickup_datetime,
                            pickup_latitude=train_data.pickup_latitude,
                            pickup_longitude=train_data.pickup_longitude,
                            dropoff_latitude=train_data.dropoff_latitude,
                            dropoff_longitude=train_data.dropoff_longitude,
                            passenger_count=train_data.passenger_count)
    db.add(_train_data)
    db.commit()
    db.refresh(_train_data)

    return _train_data


def remove_train_data(db: Session, key: str) -> None:
    _train_data = get_train_data_by_key(db, key)
    if _train_data:
        db.delete(_train_data)
        db.commit()


def update_train_data(db: Session,
                      key: str,
                      fare_amount: Decimal,
                      pickup_datetime: str,
                      pickup_latitude: Decimal,
                      pickup_longitude: Decimal,
                      dropoff_latitude: Decimal,
                      dropoff_longitude: Decimal,
                      passenger_count: int) -> Optional[TrainData]:
    _train_data = get_train_data_by_key(db, key)
    if _train_data:
        _train_data.fare_amount = fare_amount
        _train_data.pickup_datetime = pickup_datetime
        _train_data.pickup_latitude = pickup_latitude
        _train_data.pickup_longitude = pickup_longitude
        _train_data.dropoff_latitude = dropoff_latitude
        _train_data.dropoff_longitude = dropoff_longitude
        _train_data.passenger_count = passenger_count

        db.commit()
        db.refresh(_train_data)

    return _train_data


# --- Taxifare Test Data ---

def get_test_data(db: Session, skip: int=0, limit: int=100) -> List[TestData]:
    return db.query(TestData).offset(skip).limit(limit).all()


def get_test_data_by_key(db: Session, key: str) -> Optional[TestData]:
    return db.query(TestData).filter(TestData.key == key).first()


def create_test_data(db: Session,
                         test_data: TestDataSchema) -> TestData:
    _test_data = TestData(key=test_data.key,
                          pickup_datetime=test_data.pickup_datetime,
                          pickup_latitude=test_data.pickup_latitude,
                          pickup_longitude=test_data.pickup_longitude,
                          dropoff_latitude=test_data.dropoff_latitude,
                          dropoff_longitude=test_data.dropoff_longitude,
                          passenger_count=test_data.passenger_count)
    db.add(_test_data)
    db.commit()
    db.refresh(_test_data)

    return _test_data

def remove_test_data(db: Session, key: str) -> None:
    _test_data = get_test_data_by_key(db, key)
    if _test_data:
        db.delete(_test_data)
        db.commit()


def update_test_data(db: Session,
                     key: str,
                     pickup_datetime: str,
                     pickup_latitude: Decimal,
                     pickup_longitude: Decimal,
                     dropoff_latitude: Decimal,
                     dropoff_longitude: Decimal,
                     passenger_count: int) -> Optional[TestData]:
    _test_data = get_test_data_by_key(db, key)
    if _test_data:
        _test_data.pickup_datetime = pickup_datetime
        _test_data.pickup_latitude = pickup_latitude
        _test_data.pickup_longitude = pickup_longitude
        _test_data.dropoff_latitude = dropoff_latitude
        _test_data.dropoff_longitude = dropoff_longitude
        _test_data.passenger_count = passenger_count

        db.commit()
        db.refresh(_test_data)

    return _test_data