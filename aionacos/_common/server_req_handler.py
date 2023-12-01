from abc import ABCMeta

from .request import Request


class ServerRequestHandler(metaclass=ABCMeta):
    def __init__(self):
        pass

    async def request_reply(self, req: Request):
        raise NotImplementedError()

    @property
    def name(self):
        return self.__class__.__name__
