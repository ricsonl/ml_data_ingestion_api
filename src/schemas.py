from typing import Optional, Generic, TypeVar, List
from decimal import Decimal
from pydantic import BaseModel, Field
from pydantic.generics import GenericModel

T = TypeVar('T')

class RawDataSchema(BaseModel):
    ID_code: str
    target: Optional[str] = None
    var: List[Decimal] = []

    class Config:
        orm_mode = True

class RequestRawData(BaseModel):
    data: RawDataSchema = Field(...)

class RequestRawDataMassive(BaseModel):
    path: str

class Response(GenericModel, Generic[T]):
    message: Optional[str]
    result: Optional[T]