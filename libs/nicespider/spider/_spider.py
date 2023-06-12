import time
from typing import List, Type

from libs.nicespider.downloader import Downloader
from libs.nicespider.handler import Handler
from libs.nicespider.queue import Queue, MemQueue
from libs.nicespider.reqresp import Request
from libs.nicespider.spider._worker import Worker
from libs.nicespider.utils import logger


class Spider:
    def __init__(self, queue: Queue = None, workers: int = 10):
        self._queue: Queue = queue or MemQueue()
        self._downloaders: List[Downloader] = []
        self._handlers: List[Handler] = []

        self._workers: int = workers

    def add_request(self, *requests: Request) -> "Spider":
        for req in requests:
            self._queue.push(req)
        return self

    def get_request(self) -> Request:
        return self._queue.pull()

    def done_request(self, request: Request, success: bool = True):
        if success:
            self._queue.success(request)
        else:
            self._queue.fail(request)

    def add_downloader(self, *downloaders: Type[Downloader]) -> "Spider":
        self._downloaders.extend([downloader(spider=self) for downloader in downloaders])
        return self

    def get_downloaders(self) -> List[Downloader]:
        return self._downloaders

    def add_handler(self, *handlers: Type[Handler]) -> "Spider":
        self._handlers.extend([handler(spider=self) for handler in handlers])
        return self

    def get_handlers(self) -> List[Handler]:
        return self._handlers

    def start(self):
        workers = [Worker(self, name="worker-{}".format(i + 1)) for i in range(self._workers)]
        for w in workers:
            w.start()

        while True:
            logger.info("workers status: {}".format([1 if w.is_alive() else 0 for w in workers]))
            time.sleep(15)
