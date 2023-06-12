import typing
from abc import ABCMeta, abstractmethod

from libs.nicespider.reqresp import Request, Response

if typing.TYPE_CHECKING:
    from libs.nicespider.spider import Spider


class Handler(metaclass=ABCMeta):
    def __init__(self, spider: "Spider"):
        self.spider = spider

    @abstractmethod
    def match(self, req: Request, resp: Response) -> bool:
        pass

    @abstractmethod
    def handle(self, req: Request, resp: Response) -> bool:
        pass
