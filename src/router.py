from fastapi import APIRouter, HTTPException
from services import RawDataService
from schemas import RequestRawData, RequestRawDataMassive, Response

router = APIRouter()

@router.post('/{table_name}/create', status_code=201)
async def create_data(table_name: str, request: RequestRawData):
    try:
        table = RawDataService.get_table(table_name)
        await RawDataService.create_data(table, request.data)
        return Response(message='Data successfully created').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@router.post('/{table_name}/load', status_code=201)
async def load_data(table_name: str, request: RequestRawDataMassive):
    try:
        table = RawDataService.get_table(table_name)
        await RawDataService.load_data(table, request.path)
        return Response(message='Data successfully loaded').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@router.get('/{table_name}', status_code=200)
async def list_data(table_name: str):
    try:
        table = RawDataService.get_table(table_name)
        result = await RawDataService.list_data(table)
        return Response(result=result).dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@router.get('/{table_name}/{key}', status_code=200)
async def get_data(table_name: str, key: str):
    try:
        table = RawDataService.get_table(table_name)
        result = await RawDataService.get_data(table, key)
    except Exception as err:
        raise HTTPException(400, detail=str(err))
    if not result:
        raise HTTPException(404, detail=f"Key '{key}' not found in table '{table_name}'")
    return Response(result=result).dict(exclude_unset=True)
    

@router.put('/{table_name}', status_code=200)
async def update_data(table_name: str, request: RequestRawData):
    try:
        table = RawDataService.get_table(table_name)
        result = await RawDataService.get_data(table, request.data.key)
        await RawDataService.update_data(table, request.data)
    except Exception as err:
        raise HTTPException(400, detail=str(err))
    if not result:
        raise HTTPException(404, detail=f"Key '{request.data.key}' not found in table '{table_name}'")
    return Response(message='Data successfully updated').dict(exclude_unset=True)
    

@router.delete('/{table_name}', status_code=200)
async def clear_data(table_name: str):
    try:
        table = RawDataService.get_table(table_name)
        await RawDataService.clear_data(table)
        return Response(message='Table successfully cleared').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@router.delete('/{table_name}/{key}', status_code=200)
async def delete_data(table_name: str, key: str):
    try:
        table = RawDataService.get_table(table_name)
        result = await RawDataService.get_data(table, key)
        await RawDataService.delete_data(table, key)
    except Exception as err:
        raise HTTPException(400, detail=str(err))
    if not result:
        raise HTTPException(404, detail=f"Key '{key}' not found in table '{table_name}'")
    return Response(message='Data successfully deleted').dict(exclude_unset=True)