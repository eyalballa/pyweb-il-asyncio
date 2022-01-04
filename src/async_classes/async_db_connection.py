import contextlib

import asyncpg


class DBConnection:
    def __init__(self, db_uri):
        self.db_uri = db_uri
        self.pool = None

    async def connect(self):
        if self.pool is None:
            self.pool = asyncpg.create_pool(self.db_uri)

    async def execute_and_get_first(self, query):
        async with self.pool.acquire() as conn:
            return await conn.fetchval(query)

    def execute_and_get_all(self, query):
        async with self.pool.acquire() as conn:
            return conn.fetch(query)

    async def close(self):
        if self.pool is not None:
            await self.pool.close()


@contextlib.asynccontextmanager
async def db_connection(db_uri):
    db = DBConnection(db_uri)
    try:
        await db.connect()
        yield db
    finally:
        await db.close()
