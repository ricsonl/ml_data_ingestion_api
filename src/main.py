from fastapi import FastAPI, status
from typing import Dict, Any
from schemas.common import Response
from controllers.raw import raw_data_router
# from controllers.stats import stats_data_router

app = FastAPI()

@app.get('/', status_code=status.HTTP_200_OK)
async def health_check() -> Dict[str, Any]:
    return Response(message='Welcome', result=None).dict(exclude_none=True)

app.include_router(router=raw_data_router)
# app.include_router(router=stats_data_router)