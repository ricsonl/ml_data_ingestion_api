from fastapi import APIRouter, HTTPException, Path, Depends
from config import SessionLocal
from sqlalchemy.orm import Session
from schemas import TrainDataSchema, TestDataSchema, RequestTrainData, RequestTestData, Response
import crud

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# --- Routes for Train data ---

@router.post('/train_data/create')
async def create_train_data(request: RequestTrainData, db: Session=Depends(get_db)):
    crud.create_train_data(db, train_data=request.parameter)
    return Response(code=200, status='Ok', message='Train data successfully created').dict(exclude_none=True)


@router.get('/train_data')
async def get_train_data(db: Session=Depends(get_db)):
    _train_data = crud.get_train_data(db, 0, 100)
    return Response(code=200, status='Ok', message='Train data successfully fetched', result=_train_data).dict(exclude_none=True)


@router.get('/train_data/{key}')
async def get_train_data_by_key(key: str, db: Session=Depends(get_db)):
    _train_data = crud.get_train_data_by_key(db, key)
    return Response(code=200, status='Ok', message='Train data successfully returned', result=_train_data).dict(exclude_none=True)


@router.post('/train_data/update')
async def get_train_data_by_key(request: RequestTrainData, db: Session=Depends(get_db)):
    _train_data = crud.update_train_data(db,
                                         key=request.parameter.key,
                                         fare_amount=request.parameter.fare_amount,
                                         pickup_datetime=request.parameter.pickup_datetime,
                                         pickup_latitude=request.parameter.pickup_latitude,
                                         pickup_longitude=request.parameter.pickup_longitude,
                                         dropoff_latitude=request.parameter.dropoff_latitude,
                                         dropoff_longitude=request.parameter.dropoff_longitude,
                                         passenger_count=request.parameter.passenger_count)
    return Response(code=200, status='Ok', message='Train data successfully updated', result=_train_data)


@router.delete('/train_data/{key}')
async def get_train_data(key: str, db: Session=Depends(get_db)):
    crud.remove_train_data(db, key=key)
    return Response(code=200, status='Ok', message='Train data successfully deleted').dict(exclude_none=True)


# --- Routes for Test data ---

@router.post('/test_data/create')
async def create_test_data(request: RequestTestData, db: Session=Depends(get_db)):
    crud.create_test_data(db, test_data=request.parameter)
    return Response(code=200, status='Ok', message='Test data successfully created').dict(exclude_none=True)


@router.get('/test_data')
async def get_test_data(db: Session=Depends(get_db)):
    _test_data = crud.get_test_data(db, 0, 100)
    return Response(code=200, status='Ok', message='Test data successfully fetched', result=_test_data).dict(exclude_none=True)


@router.get('/test_data/{key}')
async def get_test_data_by_key(key: str, db: Session=Depends(get_db)):
    _test_data = crud.get_test_data_by_key(db, key)
    return Response(code=200, status='Ok', message='Test data successfully returned', result=_test_data).dict(exclude_none=True)


@router.post('/test_data/update')
async def get_test_by_key(request: RequestTestData, db: Session=Depends(get_db)):
    _test_data = crud.update_test_data(db,
                                       key=request.parameter.key,
                                       pickup_datetime=request.parameter.pickup_datetime,
                                       pickup_latitude=request.parameter.pickup_latitude,
                                       pickup_longitude=request.parameter.pickup_longitude,
                                       dropoff_latitude=request.parameter.dropoff_latitude,
                                       dropoff_longitude=request.parameter.dropoff_longitude,
                                       passenger_count=request.parameter.passenger_count)
    return Response(code=200, status='Ok', message='Test data successfully updated', result=_test_data)


@router.delete('/test_data/{key}')
async def get_test_data(key: str, db: Session=Depends(get_db)):
    crud.remove_test_data(db, key=key)
    return Response(code=200, status='Ok', message='Test data successfully deleted').dict(exclude_none=True)