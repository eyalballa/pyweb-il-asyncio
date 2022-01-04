import os
os.environ['PYTHONASYNCIODEBUG'] = '1'
import asyncio
from asyncio import shield, CancelledError


async def do_stuff():
    print('start shield')
    ## do stuff
    print('end shield')


async def main():
    try:
        do_stuff()
    except CancelledError:
        print('cancelled')


if __name__ == '__main__':
    asyncio.run(main())