from typing import Optional
from fastapi import APIRouter, HTTPException
from services import RawDataService
from schemas import RequestRawData, RequestRawDataMassive, Response
from datetime import datetime
from fastapi import Depends, HTTPException, status, APIRouter
# from pymongo.collection import ReturnDocument
# from app import schemas
# from app.database import Post
# from app.serializers.postSerializers import postEntity, postListEntity
# from bson.objectid import ObjectId
# from pymongo.errors import DuplicateKeyError

router = APIRouter()

@router.post('/{collection_name}/create', status_code=201)
def create_data(collection_name: str, request: RequestRawData):
    try:
        RawDataService.create_data(collection_name, request.data)
        return Response(message=f'Data successfully created').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


# @router.post('/{table_name}/load', status_code=201)
# async def load_data(table_name: str, request: RequestRawDataMassive):
#     try:
#         table = RawDataService.get_table(table_name)
#         await RawDataService.load_data(table, request.path)
#         return Response(message='Data successfully loaded').dict(exclude_unset=True)
#     except Exception as err:
#         raise HTTPException(400, detail=str(err))


@router.get('/{collection_name}/', status_code=200)
def list_data(collection_name: str, limit: Optional[int]=100):
    try:
        result = RawDataService.list_data(collection_name, limit)
        return Response(result=result).dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@router.get('/{collection_name}/{id_code}', status_code=200)
def get_data(collection_name: str, id_code: str):
    try:
        result = RawDataService.get_data(collection_name, id_code)
        return Response(result=result).dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))
    

@router.put('/{collection_name}', status_code=200)
def update_data(collection_name: str, request: RequestRawData):
    try:
        RawDataService.update_data(collection_name, request.data)
        return Response(message='Data successfully updated').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))
    

# @router.delete('/{table_name}', status_code=200)
# async def clear_data(table_name: str):
#     try:
#         table = RawDataService.get_table(table_name)
#         await RawDataService.clear_data(table)
#         return Response(message='Table successfully cleared').dict(exclude_unset=True)
#     except Exception as err:
#         raise HTTPException(400, detail=str(err))


# @router.delete('/{table_name}/{key}', status_code=200)
# async def delete_data(table_name: str, key: str):
#     try:
#         table = RawDataService.get_table(table_name)
#         result = await RawDataService.get_data(table, key)
#         await RawDataService.delete_data(table, key)
#     except Exception as err:
#         raise HTTPException(400, detail=str(err))
#     if not result:
#         raise HTTPException(404, detail=f"Key '{key}' not found in table '{table_name}'")
#     return Response(message='Data successfully deleted').dict(exclude_unset=True)