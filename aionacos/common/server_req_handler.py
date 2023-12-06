from .payload import SlotsMeta
from .request import Request


class ServerRequestHandler(metaclass=SlotsMeta):
    async def request_reply(self, req: Request):
        raise NotImplementedError()

    @property
    def name(self):
        return self.__class__.__name__
