from asyncio import run
from database.connection import engine
from database.models import Base

async def create_database():
    async with engine.begin() as conn:
        print(conn)
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

if __name__ == '__main__':
    run(create_database())