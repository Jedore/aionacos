from typing import TypeVar

from .enum import EnumBase
from .payload import RegistryMeta
from .utils import Serializable

__all__ = [
    "ResponseType",
    "Response",
    "ResponseCode",
    "ErrorResponse",
    "HealthCheckResponse",
    "ServerCheckResponse",
    "ServerLoadResponse",
    "ServerLoaderInfoResponse",
    "ConnectResetResponse",
    "ClientDetectionResponse",
]


class ResponseCode(EnumBase):
    SUCCESS = (200, "Response ok")
    FAIL = (500, "Response fail")


class Response(Serializable, metaclass=RegistryMeta):
    def __init__(
        self,
        errorCode: int = None,
        requestId: str = "",
        resultCode: int = ResponseCode.SUCCESS.code,
        message: str = "",
        success: bool = None,
    ):
        self.requestId = requestId
        self.resultCode = resultCode
        self.message = message
        self.errorCode = errorCode
        self.success = success or self.resultCode == ResponseCode.SUCCESS.code

    @property
    def cls_name(self):
        return self.__class__.__name__


ResponseType = TypeVar("ResponseType", bound=Response)


class HealthCheckResponse(Response):
    pass


class ServerCheckResponse(Response):
    def __init__(self, connectionId: str = "", **kwargs):
        super().__init__(**kwargs)

        self.connectionId = connectionId


class ServerLoadResponse(Response):
    pass


class ServerLoaderInfoResponse(Response):
    def __init__(
        self,
        loaderMetrics: dict = None,
        address: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.address = address
        self.loaderMetrics = loaderMetrics or {}


class ErrorResponse(Response):
    pass


class ConnectResetResponse(Response):
    pass


class ClientDetectionResponse(Response):
    pass
