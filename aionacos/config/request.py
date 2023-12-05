from typing import List

from ..common import constants as const
from ..common.request import Request, ServerRequest
from ..common.utils import Serializable

__all__ = [
    "AbstractConfigRequest",
    "ConfigQueryRequest",
    "ConfigListenContext",
    "ConfigRemoveRequest",
    "ConfigPublishRequest",
    "ConfigBatchListenRequest",
    "ConfigChangeNotifyRequest",
    "ClientConfigMetricRequest",
]


class AbstractConfigRequest(Request):
    # def __init__(self, **kwargs):
    #     super().__init__(**kwargs)
    #
    #     self.module = self.module or const.Config.CONFIG_MODULE

    def get_module(self):
        return const.Config.CONFIG_MODULE


class ConfigListenContext(Serializable):
    def __init__(
        self,
        dataId: str = "",
        group: str = "",
        md5: str = "",
        tenant: str = "",
    ):
        self.dataId = dataId
        self.group = group
        self.md5 = md5
        self.tenant = tenant


class ConfigBatchListenRequest(AbstractConfigRequest):
    def __init__(self, listen: bool = True, **kwargs):
        super().__init__(**kwargs)

        self.listen = listen
        self.configListenContexts: List[ConfigListenContext] = []


class ConfigChangeNotifyRequest(ServerRequest):
    def __init__(
        self,
        dataId: str = None,
        group: str = None,
        tenant: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.dataId = dataId
        self.group = group
        self.tenant = tenant

    def get_module(self):
        return const.Config.CONFIG_MODULE


class ConfigPublishRequest(AbstractConfigRequest):
    def __init__(
        self,
        dataId: str = None,
        group: str = None,
        addition_map: dict = None,
        tenant: str = None,
        content: str = None,
        cas_md5: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.dataId = dataId
        self.group = group
        self.tenant = tenant
        self.content = content
        self.casMd5 = cas_md5
        self.additionMap = addition_map or {}


class ConfigQueryRequest(AbstractConfigRequest):
    def __init__(
        self,
        dataId: str = None,
        group: str = None,
        tag: str = None,
        tenant: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.dataId = dataId
        self.group = group
        self.tenant = tenant
        self.tag = tag

    @property
    def notify(self):
        notify_ = self.headers.get(const.Config.NOTIFY_HEADER, "false")
        return notify_.lower() == "true"


class ConfigRemoveRequest(AbstractConfigRequest):
    def __init__(
        self,
        dataId: str = None,
        group: str = None,
        tag: str = None,
        tenant: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.dataId = dataId
        self.group = group
        self.tenant = tenant
        self.tag = tag


class ClientConfigMetricRequest(ServerRequest):
    def get_module(self):
        return const.Config.CONFIG_MODULE
