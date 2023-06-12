import queue
import sys

from libs.nicespider.queue._queue import Queue
from libs.nicespider.reqresp import Request
from libs.nicespider.utils import logger


class MemQueue(Queue):
    def __init__(self, maxsize: int = sys.maxsize):
        self.maxsize = maxsize
        self.queue = queue.Queue(maxsize=self.maxsize)

    def pull(self) -> Request:
        return self.queue.get()

    def push(self, req: Request):
        self.queue.put(req)

    def success(self, req: Request):
        logger.info(f"Success: {req.url}")

    def fail(self, req: Request):
        logger.error(f"Fail: {req.url}")
        self.push(req=req)
