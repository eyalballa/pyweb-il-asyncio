import asyncio


async def hello_world():
    print('hello')
    await asyncio.sleep(3)
    print('world')


if __name__ == '__main__':
    asyncio.run(hello_world())
