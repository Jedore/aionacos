from typing import List

from .pojo import ServiceInfo
from ..common.response import Response
from ..common.response_code import ResponseCode

__all__ = [
    "InstanceResponse",
    "BatchInstanceResponse",
    "ServiceListResponse",
    "QueryServiceResponse",
    "SubscribeServiceResponse",
    "NotifySubscriberResponse",
]


class NamingResponseCode(ResponseCode):
    RESOURCE_NOT_FOUND = 20404  # Resource not found
    NO_NEED_RETRY = 21600  # No need retry


class InstanceResponse(Response):
    def __init__(self, type_: str = None, **kwargs):
        super().__init__(**kwargs)
        self.type = type_


class BatchInstanceResponse(Response):
    def __init__(self, type_: str = None, **kwargs):
        super().__init__(**kwargs)
        self.type = type_


class SubscribeServiceResponse(Response):
    def __init__(self, serviceInfo: dict = None, **kwargs):
        super().__init__(**kwargs)

        if serviceInfo is None:
            self.serviceInfo = None
        else:
            self.serviceInfo = ServiceInfo(**serviceInfo)


class QueryServiceResponse(Response):
    def __init__(self, serviceInfo: dict = None, **kwargs):
        super().__init__(**kwargs)

        if serviceInfo is None:
            self.serviceInfo = None
        else:
            self.serviceInfo = ServiceInfo(**serviceInfo)


class NotifySubscriberResponse(Response):
    pass


class ServiceListResponse(Response):
    def __init__(
        self,
        count: int = 0,
        serviceNames: List[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.count = count
        self.serviceNames = serviceNames or []
