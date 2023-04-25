from typing import Optional, Generic, TypeVar
from pydantic import BaseModel, Field, Extra, root_validator
from pydantic.generics import GenericModel
import numbers

T = TypeVar('T')
MAX_VARS = 200

class RawDataSchema(BaseModel):
    ID_code: str
    target: Optional[int] = None

    @root_validator(pre=True)
    def validate_values(cls, values):
        vars = {k:v for k,v in values.items() if k.startswith('var_')}
        if len(vars) > MAX_VARS:
            raise ValueError(f"Too many 'var_' variables. Maximum is {MAX_VARS}")
        if not all(isinstance(vars[x], numbers.Number) for x in vars):
            raise ValueError("'var_' variables must be numeric")
        vars.update({'ID_code': values.get('ID_code'), 'target': values.get('target')})
        return vars

    class Config:
        orm_mode = True
        extra = Extra.allow

class RequestRawData(BaseModel):
    data: RawDataSchema = Field(...)

class RequestRawDataMassive(BaseModel):
    path: str

class Response(GenericModel, Generic[T]):
    message: Optional[str]
    result: Optional[T]