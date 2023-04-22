from fastapi import FastAPI
from router import raw_data_router

app = FastAPI()

@app.get('/')
async def Home():
    return "Welcome"

app.include_router(router=raw_data_router)