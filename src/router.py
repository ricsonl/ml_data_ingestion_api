from fastapi import APIRouter, HTTPException
from services import RawDataService
from schemas import RequestRawData, RequestRawDataMassive, Response

router = APIRouter()

@router.post('/{table_name}/create')
async def create_data(table_name: str, request: RequestRawData):
    try:
        table = RawDataService.get_table(table_name)
        await RawDataService.create_data(table, request.data)
        return Response(code=200, status='OK', message='Data successfully created').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@router.post('/{table_name}/load')
async def load_data(table_name: str, request: RequestRawDataMassive):
    try:
        table = RawDataService.get_table(table_name)
        await RawDataService.load_data(table, request.path)
        return Response(code=200, status='OK', message='Data successfully loaded').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@router.get('/{table_name}')
async def list_data(table_name: str):
    try:
        table = RawDataService.get_table(table_name)
        result = await RawDataService.list_data(table)
        return Response(code=200, status='OK', result=result).dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@router.get('/{table_name}/{key}')
async def get_data(table_name: str, key: str):
    try:
        table = RawDataService.get_table(table_name)
        result = await RawDataService.get_data(table, key)
        if not result:
            return Response(code=404, status='Not Found', message=f"Key '{key}' not found in table").dict(exclude_unset=True)
        return Response(code=200, status='OK', result=result).dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))
    

@router.put('/{table_name}')
async def update_data(table_name: str, request: RequestRawData):
    try:
        table = RawDataService.get_table(table_name)
        result = await RawDataService.get_data(table, request.data.key)
        if not result:
            return Response(code=404, status='Not Found', message=f"Key '{request.data.key}' not found in table").dict(exclude_unset=True)
        await RawDataService.update_data(table, request.data)
        return Response(code=200, status='OK', message='Data successfully updated').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))
    

@router.delete('/{table_name}')
async def clear_data(table_name: str):
    try:
        table = RawDataService.get_table(table_name)
        await RawDataService.clear_data(table)
        return Response(code=200, status='OK', message='Table successfully cleared').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@router.delete('/{table_name}/{key}')
async def delete_data(table_name: str, key: str):
    try:
        table = RawDataService.get_table(table_name)
        result = await RawDataService.get_data(table, key)
        if not result:
            return Response(code=404, status='Not Found', message=f"Key '{key}' not found in table").dict(exclude_unset=True)
        await RawDataService.delete_data(table, key)
        return Response(code=200, status='OK', message='Data successfully deleted').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))