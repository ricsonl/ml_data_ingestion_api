from fastapi import FastAPI
from router import train_data_router, test_data_router

app = FastAPI()

@app.get('/')
async def Home():
    return "Welcome"

app.include_router(router=train_data_router)
app.include_router(router=test_data_router)