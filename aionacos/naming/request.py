from .._common import constants as cst
from .._common.request import Request, ServerRequest
from ..naming.pojo import Instance, ServiceInfo

__all__ = [
    "NamingRequest",
    "InstanceRequest",
    "ServiceListRequest",
    "ServiceQueryRequest",
    "BatchInstanceRequest",
    "SubscribeServiceRequest",
    "NotifySubscriberRequest",
]


class NamingRequest(Request):
    def __init__(
        self,
        serviceName: str = "",
        groupName: str = "",
        namespace: str = "",
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.namespace = namespace
        self.serviceName = serviceName
        self.groupName = groupName

    def get_module(self):
        return cst.Naming.NAMING_MODULE


class InstanceRequest(NamingRequest):
    def __init__(
        self,
        type_: str = "",
        instance: Instance = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.type = type_
        self.instance = instance


class BatchInstanceRequest(NamingRequest):
    def __init__(
        self,
        type_: str = "",
        instances: list = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.type = type_
        self.instances = instances or []


class ServiceListRequest(NamingRequest):
    def __init__(
        self,
        pageNo: int,
        pageSize: int,
        selector: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.pageNo = pageNo
        self.pageSize = pageSize
        self.selector = selector


class ServiceQueryRequest(NamingRequest):
    def __init__(
        self,
        healthyOnly: bool,
        udpPort: int,
        cluster: str,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.udpPort = udpPort
        self.cluster = cluster
        self.healthyOnly = healthyOnly


class SubscribeServiceRequest(NamingRequest):
    def __init__(
        self,
        subscribe: bool,
        clusters: str,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.subscribe = subscribe
        self.clusters = clusters


class NotifySubscriberRequest(ServerRequest):
    def __init__(
        self,
        serviceInfo: dict = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.namespace = ""
        self.serviceName = ""
        self.groupName = ""
        if serviceInfo is None:
            self.serviceInfo = None
        else:
            self.serviceInfo = ServiceInfo(**serviceInfo)

    def get_module(self):
        return cst.Naming.NAMING_MODULE
