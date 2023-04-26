from typing import Optional, Generic, TypeVar
from pydantic import BaseModel, Field, Extra, root_validator 
from pydantic.generics import GenericModel
import numbers

T = TypeVar('T')
MAX_VARS = 200

class RawDataSchema(BaseModel):
    ID_code: Optional[str] = None
    target: Optional[int] = None

    @root_validator(pre=True)
    def validate_values(cls, values):
        if any((key not in ['ID_code', 'target'] and not key.startswith('var_')) for key in values.keys()):
            raise ValueError(f"The only accepted field names are 'ID_code', 'target' or starting with 'var_'")
        
        target = values.get('target')
        if target != None and target not in [0, 1]:
            raise ValueError(f"'target' must be 0 or 1")
        
        vars = {k:v for k,v in values.items() if k.startswith('var_')}
        if len(vars) > MAX_VARS:
            raise ValueError(f"Too many 'var_' variables. Maximum is {MAX_VARS}")
        
        if not all(isinstance(vars[key], numbers.Number) for key in vars):
            raise ValueError("'var_' variables must be numeric")
        return values

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