import queue
import sys

from libs.crawlite._core import Queue, Request


class MemQueue(Queue):
    def __init__(self, maxsize: int = sys.maxsize):
        self.maxsize = maxsize
        self.queue = queue.Queue(maxsize=self.maxsize)

    def pull(self) -> Request:
        return self.queue.get()

    def push(self, req: Request):
        self.queue.put(req)

    def success(self, req: Request):
        print(f"Success: {req.url}")

    def fail(self, req: Request):
        print(f"Fail: {req.url}")

    def size(self) -> int:
        return self.queue.qsize()
