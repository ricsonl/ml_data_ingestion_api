from fastapi import FastAPI
from controllers.raw import raw_data_router

app = FastAPI()

@app.get('/')
async def health_check():
    return "Welcome"

app.include_router(router=raw_data_router)