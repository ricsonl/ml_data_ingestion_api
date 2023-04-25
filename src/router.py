from typing import Optional
from fastapi import APIRouter, HTTPException
from services import RawDataService
from schemas import RequestRawData, RequestRawDataMassive, Response

router = APIRouter(prefix='/{collection_name}')

@router.post('/', status_code=201)
def create_data(collection_name: str, request: RequestRawData):
    try:
        RawDataService.validate_collection_name(collection_name)
        RawDataService.create_data(collection_name, request.data)
        return Response(message=f'Data successfully created').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@router.get('/', status_code=200)
def list_data(collection_name: str, limit: Optional[int]=0):
    try:
        RawDataService.validate_collection_name(collection_name)
        result = RawDataService.list_data(collection_name, limit)
        return Response(result=result).dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@router.get('/{id_code}', status_code=200)
def get_data(collection_name: str, id_code: str):
    try:
        RawDataService.validate_collection_name(collection_name)
        result = RawDataService.get_data(collection_name, id_code)
        return Response(result=result).dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))
    

@router.put('/', status_code=201)
def update_data(collection_name: str, request: RequestRawData):
    try:
        RawDataService.validate_collection_name(collection_name)
        RawDataService.update_data(collection_name, request.data)
        return Response(message='Data successfully updated').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))
    

@router.delete('/clear', status_code=200)
def clear_collection(collection_name: str):
    try:
        RawDataService.validate_collection_name(collection_name)
        RawDataService.clear_collection(collection_name)
        return Response(message='Collection successfully cleared').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@router.delete('/{id_code}', status_code=200)
def delete_data(collection_name: str, id_code: str):
    try:
        RawDataService.validate_collection_name(collection_name)
        RawDataService.delete_data(collection_name, id_code)
        return Response(message='Data successfully deleted').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@router.post('/massive', status_code=201)
def load_data(collection_name: str, request: RequestRawDataMassive):
    try:
        RawDataService.validate_collection_name(collection_name)
        RawDataService.load_data(collection_name, request.path)
        return Response(message='Data successfully loaded').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))