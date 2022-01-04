import contextlib

from src.data_classes.data_classes_for_integration import QueueItem


class Queue:
    def __init__(self, queue_uri: str):
        self.queue_uri = queue_uri

    async def connect(self):
        # connect to the queue
        pass

    async def get_item(self):
        # get the first message on the queue
        queue_result = {}
        return QueueItem.parse_obj(queue_result)

    async def close(self):
        # cleanup
        pass


@contextlib.asynccontextmanager
async def queue_conn(queue_uri: str) -> Queue:
    queue = Queue(queue_uri)
    try:
        await queue.connect()
        yield queue
    finally:
        await queue.close()
