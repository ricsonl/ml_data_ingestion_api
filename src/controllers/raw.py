from typing import Optional
from fastapi import APIRouter, HTTPException
from services.raw import RawDataService
from schemas.raw import RequestRawData, RequestRawDataMassive, Response
from database.connection import get_instance

raw_data_router = APIRouter(prefix='/{collection_name}')

@raw_data_router.post('/', status_code=201)
def create_data(collection_name: str, request: RequestRawData):
    try:
        db = get_instance().db
        RawDataService.validate_collection_name(collection_name)
        RawDataService.create_data(db, collection_name, request.data)
        return Response(message=f'Data successfully created').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@raw_data_router.get('/', status_code=200)
def list_data(collection_name: str, limit: Optional[int]=0):
    try:
        db = get_instance().db
        RawDataService.validate_collection_name(collection_name)
        result = RawDataService.list_data(db, collection_name, limit)
        return Response(result=result).dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@raw_data_router.get('/{id_code}', status_code=200)
def get_data(collection_name: str, id_code: str):
    try:
        db = get_instance().db
        RawDataService.validate_collection_name(collection_name)
        result = RawDataService.get_data(db, collection_name, id_code)
        return Response(result=result).dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))  
    

@raw_data_router.put('/', status_code=201)
def update_data(collection_name: str, request: RequestRawData):
    try:
        db = get_instance().db
        RawDataService.validate_collection_name(collection_name)
        RawDataService.update_data(db, collection_name, request.data)
        return Response(message='Data successfully updated').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))
    

@raw_data_router.delete('/clear', status_code=200)
def clear_collection(collection_name: str):
    try:
        db = get_instance().db
        RawDataService.validate_collection_name(collection_name)
        RawDataService.clear_collection(db, collection_name)
        return Response(message='Collection successfully cleared').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@raw_data_router.delete('/{id_code}', status_code=200)
def delete_data(collection_name: str, id_code: str):
    try:
        db = get_instance().db
        RawDataService.validate_collection_name(collection_name)
        RawDataService.delete_data(db, collection_name, id_code)
        return Response(message='Data successfully deleted').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@raw_data_router.post('/massive', status_code=201)
def load_data(collection_name: str, request: RequestRawDataMassive):
    try:
        db = get_instance().db
        RawDataService.validate_collection_name(collection_name)
        RawDataService.load_data(db, collection_name, request.path)
        return Response(message='Data successfully loaded').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))