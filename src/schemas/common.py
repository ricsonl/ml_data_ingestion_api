from typing import Optional, Generic, TypeVar
from pydantic.generics import GenericModel

T = TypeVar('T')

class Response(GenericModel, Generic[T]):
    message: Optional[str]
    result: Optional[T]