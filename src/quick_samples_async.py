import asyncio

import aioboto3

from async_classes.async_db_connection import db_connection, DBConnection


async def read_from_s3():
    bucket = 'bucket'
    filename = 'filename'
    session = aioboto3.Session()
    s3 = await session.client("s3")
    s3_ob = await s3.get_object(Bucket=bucket, Key=filename)
    stream = await s3_ob["Body"]
    contents = await stream.read()
    print(contents.decode("utf-8"))


async def query_pg():
    uri = 'db_uri'
    query = 'select * from TABLE'
    db = DBConnection(uri)
    try:
        conn = await db.connect()
        items = await conn.execute_and_get_all(query)
        for item in items:
            print(item)
    finally:
        await db.close()


async def main():
    s3_read_task = asyncio.create_task(read_from_s3())
    query_task = asyncio.create_task(query_pg())
    await asyncio.gather(s3_read_task, query_task)


if __name__ == '__main__':
    asyncio.run(main())
