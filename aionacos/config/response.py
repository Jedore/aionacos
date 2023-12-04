from typing import List

from ..common.response import Response
from ..config.request import ConfigListenContext

__all__ = [
    "ConfigQueryResponse",
    "ConfigRemoveResponse",
    "ConfigPublishResponse",
    "ConfigChangeNotifyResponse",
    "ClientConfigMetricResponse",
    "ConfigChangeBatchListenResponse",
]


class ConfigChangeBatchListenResponse(Response):
    def __init__(
        self,
        changedConfigs: List[dict] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.changedConfigs: List[ConfigListenContext] = []
        if changedConfigs:
            for config in changedConfigs:
                self.changedConfigs.append(ConfigListenContext(**config))


class ConfigChangeNotifyResponse(Response):
    pass


class ConfigPublishResponse(Response):
    pass


class ConfigQueryResponse(Response):
    CONFIG_NOT_FOUND = 300
    CONFIG_QUERY_CONFLICT = 400

    def __init__(
        self,
        content: str = None,
        md5: str = None,
        tag: str = None,
        lastModified: int = 0,
        contentType: str = None,
        encryptedDataKey: str = None,
        beta: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.content = content
        self.contentType = contentType
        self.md5 = md5
        self.tag = tag
        self.lastModified = lastModified
        self.beta = beta
        self.encryptedDataKey = encryptedDataKey


class ConfigRemoveResponse(Response):
    pass


class ClientConfigMetricResponse(Response):
    pass
