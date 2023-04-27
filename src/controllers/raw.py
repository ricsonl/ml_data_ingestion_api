from typing import Optional, Dict, Any
from fastapi import APIRouter, HTTPException
from services.raw import RawDataService
from schemas.raw import RequestRawData, RequestRawDataMassive, Response
from database.connection import MongoManager

raw_data_router = APIRouter(prefix='/{collection_name}')

@raw_data_router.post('/', status_code=201)
def create_data(collection_name: str, request: RequestRawData) -> Optional[Dict[str, Any]]:
    try:
        db = MongoManager().db
        RawDataService.validate_collection_name(db, collection_name)
        RawDataService.create_data(db, collection_name, request.data)
        return Response(message=f'Data successfully created', result=None).dict(exclude_none=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@raw_data_router.get('/', status_code=200)
def list_data(collection_name: str, limit: int=0) -> Optional[Dict[str, Any]]:
    try:
        db = MongoManager().db
        RawDataService.validate_collection_name(db, collection_name)
        result = RawDataService.list_data(db, collection_name, limit)
        return Response(message=None, result=result).dict(exclude_none=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@raw_data_router.get('/{id_code}', status_code=200)
def get_data(collection_name: str, id_code: str) -> Optional[Dict[str, Any]]:
    try:
        db = MongoManager().db
        RawDataService.validate_collection_name(db, collection_name)
        result = RawDataService.get_data(db, collection_name, id_code)
        return Response(message=None, result=result).dict(exclude_none=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))  
    

@raw_data_router.put('/', status_code=201)
def update_data(collection_name: str, request: RequestRawData) -> Optional[Dict[str, Any]]:
    try:
        db = MongoManager().db
        RawDataService.validate_collection_name(db, collection_name)
        RawDataService.update_data(db, collection_name, request.data)
        return Response(message='Data successfully updated', result=None).dict(exclude_none=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))
    

@raw_data_router.delete('/clear', status_code=200)
def clear_collection(collection_name: str) -> Optional[Dict[str, Any]]:
    try:
        db = MongoManager().db
        RawDataService.validate_collection_name(db, collection_name)
        RawDataService.clear_collection(db, collection_name)
        return Response(message='Collection successfully cleared', result=None).dict(exclude_none=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@raw_data_router.delete('/{id_code}', status_code=200)
def delete_data(collection_name: str, id_code: str) -> Optional[Dict[str, Any]]:
    try:
        db = MongoManager().db
        RawDataService.validate_collection_name(db, collection_name)
        RawDataService.delete_data(db, collection_name, id_code)
        return Response(message='Data successfully deleted', result=None).dict(exclude_none=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@raw_data_router.post('/massive', status_code=201)
def load_data(collection_name: str, request: RequestRawDataMassive) -> Optional[Dict[str, Any]]:
    try:
        db = MongoManager().db
        RawDataService.validate_collection_name(db, collection_name)
        RawDataService.load_data(db, collection_name, request.path)
        return Response(message='Data successfully loaded', result=None).dict(exclude_none=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))