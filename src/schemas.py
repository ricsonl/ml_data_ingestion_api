from typing import Optional, Generic, TypeVar
from decimal import Decimal
from datetime import datetime
from pydantic import BaseModel, Field
from pydantic.generics import GenericModel

T = TypeVar('T')

class RawDataSchema(BaseModel):
    key: Optional[str]=None
    fare_amount: Optional[Decimal]=None
    pickup_datetime: Optional[datetime]=None
    pickup_latitude: Optional[Decimal]=None
    pickup_longitude: Optional[Decimal]=None
    dropoff_latitude: Optional[Decimal]=None
    dropoff_longitude: Optional[Decimal]=None
    passenger_count: Optional[int]=None

    class Config:
        orm_mode = True


class RequestRawData(BaseModel):
    data: RawDataSchema = Field(...)


class RequestRawDataMassive(BaseModel):
    path: str


class Response(GenericModel, Generic[T]):
    code: str
    status: str
    message: Optional[str]
    result: Optional[T]