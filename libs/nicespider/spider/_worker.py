import threading
import typing

from libs.nicespider.utils import logger

if typing.TYPE_CHECKING:
    from libs.nicespider.spider import Spider
    from libs.nicespider.reqresp import Request, Response


class Worker(threading.Thread):
    def __init__(self, spider: "Spider", name=""):
        super().__init__(name=name, daemon=True)
        self.spider = spider

    def download(self, req: "Request"):
        for downloader in self.spider.get_downloaders():
            if downloader.match(req):
                return downloader.download(req)
        raise Exception("not found downloader: url={}".format(req.url))

    def handle(self, req: "Request", resp: "Response"):
        for handler in self.spider.get_handlers():
            if handler.match(req, resp):
                return handler.handle(req, resp)
        raise Exception("not found handler: url={}".format(req.url))

    def run(self) -> None:
        while True:
            req = None
            try:
                req = self.spider.get_request()
                resp = self.download(req)
                success = self.handle(req, resp)

                if not success:
                    raise Exception("process failed for request: url={}".format(req.url))

                self.spider.done_request(req)
            except Exception as e:
                logger.error("worker error: {}".format(e))
                if req:
                    self.spider.done_request(req, success=False)
