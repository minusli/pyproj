import typing
from abc import ABC, abstractmethod

from libs.nicespider.reqresp import Request, Response

if typing.TYPE_CHECKING:
    from libs.nicespider.spider import Spider


class Downloader(ABC):
    def __init__(self, spider: "Spider"):
        self.spider = spider

    @abstractmethod
    def match(self, req: Request) -> bool:
        pass

    @abstractmethod
    def download(self, req: Request) -> Response:
        pass
