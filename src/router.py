from fastapi import APIRouter, HTTPException
from services import TrainDataService, TestDataService
from schemas import RequestTrainData, RequestTestData, Response

train_data_router = APIRouter(prefix='/train_data')
test_data_router = APIRouter(prefix='/test_data')

# --- Routes for Train data ---
@train_data_router.post('/create')
async def create_train_data(request: RequestTrainData):
    try:
        await TrainDataService.create_train_data(request.parameter)
        return Response(code=200, status='Ok', message='Train data successfully created').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))
    

@train_data_router.get('/')
async def list_train_data():
    try:
        train_data = await TrainDataService.list_train_data()
        return Response(code=200, status='Ok', result=train_data).dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@train_data_router.get('/{key}')
async def get_train_data(key: str):
    try:
        train_data = await TrainDataService.get_train_data(key)
        return Response(code=200, status='Ok', result=train_data).dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))
    

@train_data_router.post('/update')
async def update_train_data(request: RequestTrainData):
    try:
        await TrainDataService.update_train_data(request.parameter)
        return Response(code=200, status='Ok', message='Train data successfully updated').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))
    

@train_data_router.delete('/{key}')
async def delete_train_data(key: str):
    try:
        await TrainDataService.delete_train_data(key)
        return Response(code=200, status='Ok', message='Train data successfully deleted').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))
    
# --- Routes for Test data ---
@test_data_router.post('/create')
async def create_test_data(request: RequestTestData):
    try:
        await TestDataService.create_test_data(request.parameter)
        return Response(code=200, status='Ok', message='Test data successfully created').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))
    

@test_data_router.get('/')
async def list_test_data():
    try:
        test_data = await TestDataService.list_test_data()
        return Response(code=200, status='Ok', result=test_data).dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))


@test_data_router.get('/{key}')
async def get_test_data(key: str):
    try:
        test_data = await TestDataService.get_test_data(key)
        return Response(code=200, status='Ok', result=test_data).dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))
    

@test_data_router.post('/update')
async def update_test_data(request: RequestTestData):
    try:
        await TestDataService.update_test_data(request.parameter)
        return Response(code=200, status='Ok', message='Test data successfully updated').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))
    

@test_data_router.delete('/{key}')
async def delete_test_data(key: str):
    try:
        await TestDataService.delete_test_data(key)
        return Response(code=200, status='Ok', message='Test data successfully deleted').dict(exclude_unset=True)
    except Exception as err:
        raise HTTPException(400, detail=str(err))