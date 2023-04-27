from typing import Dict, Any, Optional, Generic, TypeVar
from pydantic import BaseModel, StrictStr, StrictInt, Extra, Field, root_validator 
from pydantic.generics import GenericModel
import numbers

T = TypeVar('T')
MAX_VARS = 200

class RawDataSchema(BaseModel):
    ID_code: StrictStr
    target: Optional[StrictInt] = None

    @root_validator(pre=True)
    def validate_values(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        vars_count = 0
        for key in values.keys():
            if key not in ['ID_code', 'target'] and not key.startswith('var_'):
                raise ValueError("The only accepted field names are 'ID_code', 'target' or starting with 'var_'")
            if key == 'target':
                val = values.get(key)
                if val != None and val not in [0, 1]:
                    raise ValueError(f"'{key}' must be 0 or 1")
            if key.startswith('var_'):
                val = values.get(key)
                if not isinstance(val, numbers.Number):
                    raise ValueError(f"'{key}' must be numeric")
                vars_count += 1
        if vars_count > MAX_VARS:
            raise ValueError(f"Too many 'var_' variables. Maximum is {MAX_VARS}")
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