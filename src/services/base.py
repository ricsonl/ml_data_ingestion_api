from pymongo.errors import CollectionInvalid
from pymongo.typings import _DocumentType
from pymongo.database import Database

class BaseService:
    @staticmethod
    def validate_collection_name(db: Database[_DocumentType], collection_name: str) -> None:
        allowed_names = db.list_collection_names()
        q = '\''
        if collection_name not in allowed_names:
            raise CollectionInvalid(f"'collection_name' must be {' or '.join([f'{q}{an}{q}' for an in allowed_names])}")