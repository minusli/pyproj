import json
import threading
from abc import ABC, abstractmethod
from typing import Any, Dict, Union, Optional, List, Type

import time


class Request:
    def __init__(self, url: str, **kwargs):
        self.url: str = url
        self.ctx: Dict[str, Any] = kwargs

    def dump(self) -> str:
        return json.dumps({'url': self.url, 'ctx': self.ctx})

    def load(self, content: str) -> "Request":
        data = json.loads(content)
        self.url = data['url']
        self.ctx = data.get('ctx', {})
        return self


class Response:
    def __init__(self, content: Union[str | bytes], **kwargs):
        self.content = content
        self.ctx: Dict[str, Any] = kwargs

    def json(self):
        return json.loads(self.content)

    def text(self, encoding='utf-8'):
        if isinstance(self.content, bytes):
            return self.content.decode(encoding)
        return self.content


class Queue(ABC):
    @abstractmethod
    def pull(self) -> Request:
        pass

    @abstractmethod
    def push(self, req: Request):
        pass

    @abstractmethod
    def success(self, req: Request):
        pass

    @abstractmethod
    def fail(self, req: Request):
        pass

    @abstractmethod
    def size(self) -> int:
        pass


class Downloader(ABC):
    def __init__(self, crawler: "Crawler"):
        self.crawler = crawler

    @abstractmethod
    def match(self, req: Request) -> bool:
        pass

    @abstractmethod
    def download(self, req: Request) -> Response:
        pass


class Handler(ABC):
    def __init__(self, crawler: "Crawler"):
        self.crawler = crawler

    @abstractmethod
    def match(self, req: Request, resp: Response) -> bool:
        pass

    @abstractmethod
    def handle(self, req: Request, resp: Response) -> bool:
        pass


class Interceptor(ABC):
    @abstractmethod
    def intercept(self, req: Request) -> bool:
        pass


class Crawler:
    def __init__(self, worker_size: int = 3):
        self.worker_size = worker_size
        self.queue: Optional[Queue] = None
        self.downloaders: List[Downloader] = []
        self.handlers: List[Handler] = []
        self.interceptors: List[Interceptor] = []

        self.workers = [Worker(self, name="worker-{}".format(i + 1)) for i in range(self.worker_size)]

    def add_downloader(self, *downloaders: Type[Downloader]) -> "Crawler":
        self.downloaders.extend([downloader(self) for downloader in downloaders])
        return self

    def add_handler(self, *handlers: Type[Handler]) -> "Crawler":
        self.handlers.extend([handler(self) for handler in handlers])
        return self

    def add_interceptor(self, *interceptors: Type[Interceptor]) -> "Crawler":
        self.interceptors.extend([interceptor() for interceptor in interceptors])
        return self

    def add_request(self, url: str, **kwargs) -> "Crawler":
        self.queue.push(Request(url, **kwargs))
        return self

    def start(self) -> "Crawler":
        for w in self.workers:
            w.start()
        threading.Thread(target=self.heartbeat).start()
        return self

    def heartbeat(self):
        while True:
            active_workers = [w for w in self.workers if w.is_alive()]
            remaining_requests = self.queue.size()
            print("active workers({}), remaining requests({})".format(len(active_workers), remaining_requests))
            time.sleep(15)


class Worker(threading.Thread):
    def __init__(self, crawler: "Crawler", name=""):
        super().__init__(name=name, daemon=True)
        self.crawler = crawler

    def download(self, req: Request) -> Response:
        for downloader in self.crawler.downloaders:
            if downloader.match(req):
                return downloader.download(req)
        raise Exception("not found downloader: url={}".format(req.url))

    def handle(self, req: Request, resp: Response) -> bool:
        for handler in self.crawler.handlers:
            if handler.match(req, resp):
                return handler.handle(req, resp)
        raise Exception("not found handler: url={}".format(req.url))

    def intercept(self, req: Request) -> bool:
        for interceptor in self.crawler.interceptors:
            if not interceptor.intercept(req):
                return False
        return True

    def run(self) -> None:
        while True:
            req = None
            try:
                req = self.crawler.queue.pull()
                if not self.intercept(req):
                    print("request intercepted: url={}".format(req.url))
                    self.crawler.queue.success(req)
                    continue

                resp = self.download(req)
                if self.handle(req, resp):
                    self.crawler.queue.success(req)
                else:
                    self.crawler.queue.fail(req)
            except Exception as e:
                print("worker error: {}".format(e))
                if req:
                    self.crawler.queue.fail(req)
