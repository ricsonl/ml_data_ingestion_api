from fastapi import APIRouter, HTTPException
from services import RawDataService
from schemas import RequestRawData, Response

raw_data_router = APIRouter(prefix='/raw')

@raw_data_router.post('/{table}/create')
async def create_data(table: str, request: RequestRawData):
    try:
        await RawDataService.create_data(table, request.parameter)
        return Response(code=200, status='Ok', message=f"Data successfully created in '{table}'").dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))
    

@raw_data_router.get('/{table}')
async def list_data(table: str):
    try:
        data = await RawDataService.list_data(table)
        return Response(code=200, status='Ok', result=data).dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@raw_data_router.get('/{table}/{key}')
async def get_data(table: str, key: str):
    try:
        train_data = await RawDataService.get_data(table, key)
        return Response(code=200, status='Ok', result=train_data).dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))
    

@raw_data_router.put('/{table}')
async def update_data(table: str, request: RequestRawData):
    try:
        await RawDataService.update_data(table, request.parameter)
        return Response(code=200, status='Ok', message=f"Data successfully updated in '{table}'").dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))
    

@raw_data_router.delete('/{table}/{key}')
async def delete_data(table: str, key: str):
    try:
        await RawDataService.delete_data(table, key)
        return Response(code=200, status='Ok', message=f"Data successfully deleted in '{table}'").dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))