from typing import Optional, Dict, Any
from fastapi import APIRouter, HTTPException, status
from schemas.common import Response
from services.stats import StatsService
from database.connection import MongoManager

stats_data_router = APIRouter(prefix='/{collection_name}/stats')

@stats_data_router.get('/', status_code=status.HTTP_200_OK)
def list_data(collection_name: str) -> Optional[Dict[str, Any]]:
    pass
    # try:
    #     db = MongoManager().db
    #     StatsService.validate_collection_name(db, collection_name)
    #     result = StatsService.get_data(db, collection_name)
    #     return Response(message=None, result=result).dict(exclude_none=True)
    # except Exception as err:
    #     raise HTTPException(400, detail=str(err))  
    

@stats_data_router.put('/', status_code=status.HTTP_201_CREATED)
def update_data(collection_name: str) -> Optional[Dict[str, Any]]:
    pass
    # try:
    #     db = MongoManager().db
    #     StatsService.validate_collection_name(db, collection_name)
    #     StatsService.update_data(db, collection_name)
    #     return Response(message='Stats successfully updated', result=None).dict(exclude_none=True)
    # except Exception as err:
    #     raise HTTPException(400, detail=str(err))