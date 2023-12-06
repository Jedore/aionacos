import typing as t

from . import constants as const
from .ability import ClientAbilities
from .payload import RegistryMeta, SlotsMeta
from .utils import Serializable

__all__ = [
    "Request",
    "RequestType",
    "ServerRequest",
    "PushAckRequest",
    "InternalRequest",
    "HealthCheckRequest",
    "ServerCheckRequest",
    "ServerReloadRequest",
    "ConnectResetRequest",
    "ConnectionSetupRequest",
    "ClientDetectionRequest",
    "ServerLoaderInfoRequest",
]


class Request(Serializable, metaclass=RegistryMeta):
    def __init__(
        self,
        requestId: str = None,
        headers: dict = None,
        module: str = None,
    ):
        self.requestId = requestId
        self.module = module
        self.headers = headers or {}

    @property
    def cls_name(self):
        return self.__class__.__name__

    def get_module(self):
        raise NotImplementedError()


RequestType = t.TypeVar("RequestType", bound=Request)


class RequestMeta(object, metaclass=SlotsMeta):
    def __init__(
        self,
        clientIp: str = "",
        clientVersion: str = "",
        connectionId: str = "",
        labels: dict = None,
    ):
        self.clientIp = clientIp
        self.clientVersion = clientVersion
        self.connectionId = connectionId
        self.labels = labels or {}


class InternalRequest(Request):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.module = self.module or const.Remote.INTERNAL_MODULE

    def get_module(self):
        return const.Remote.INTERNAL_MODULE


class HealthCheckRequest(InternalRequest):
    pass


class ServerCheckRequest(InternalRequest):
    pass


class ServerLoaderInfoRequest(InternalRequest):
    pass


class ServerReloadRequest(InternalRequest):
    def __int__(self, reloadCount: int, reloadServer: str):
        self.reloadServer = reloadServer
        self.reloadCount = reloadCount


class ConnectionSetupRequest(InternalRequest):
    def __init__(
        self,
        clientVersion: str,
        tenant: str = None,
        labels: dict = None,
        abilities: ClientAbilities = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.clientVersion = clientVersion
        self.tenant = tenant
        self.labels = labels or dict()
        self.abilities = abilities


class PushAckRequest(InternalRequest):
    # todo
    pass


class ServerRequest(Request):
    pass


class ConnectResetRequest(ServerRequest):
    def __init__(
        self,
        serverIp: str = None,
        serverPort: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.serverIp = serverIp
        self.serverPort = serverPort

    def get_module(self):
        return const.Remote.INTERNAL_MODULE


class ClientDetectionRequest(ServerRequest):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_module(self):
        return const.Remote.INTERNAL_MODULE
