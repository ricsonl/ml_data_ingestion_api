from fastapi import FastAPI
from router import router

app = FastAPI()

@app.get('/')
async def Home():
    return "Welcome"

app.include_router(router=router)