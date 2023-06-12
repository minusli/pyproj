from abc import ABC, abstractmethod

from libs.nicespider.reqresp import Request


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
