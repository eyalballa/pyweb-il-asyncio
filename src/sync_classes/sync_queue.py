import contextlib

from src.data_classes.data_classes_for_integration import QueueItem


class Queue:
    def __init__(self, queue_uri: str):
        self.queue_uri = queue_uri

    def connect(self):
        # connect to the queue
        pass

    def get_item(self):
        # get the first message on the queue
        queue_result = {}
        return QueueItem.parse_obj(queue_result)

    def close(self):
        # cleanup
        pass


@contextlib.contextmanager
def queue_conn(queue_uri: str) -> Queue:
    queue = Queue(queue_uri)
    try:
        queue.connect()
        yield queue
    finally:
        queue.close()
