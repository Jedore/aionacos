from typing import Dict, Callable

from . import group_key
from .cache_data import CacheData
from .request import *
from .response import *
from .. import _utils
from .._common.request import Request
from .._common.server_request_handler import ServerRequestHandler


class ConfigPushRequestHandler(ServerRequestHandler):
    def __init__(
        self, notify_listen_config: Callable, cache_map: Dict[str, CacheData]
    ):
        super().__init__()
        self._notify_listen_config = notify_listen_config
        self._cache_map = cache_map

    async def request_reply(self, req: Request):
        if isinstance(req, ConfigChangeNotifyRequest):
            key = group_key.get_key_tenant(req.dataId, req.group, req.tenant)
            cache = self._cache_map.get(key)
            if cache:
                cache._last_modified_time = _utils.timestamp()
                cache._is_sync_with_server = False
                self._notify_listen_config()
            return ConfigChangeNotifyResponse()
        elif isinstance(req, ClientConfigMetricRequest):
            # todo
            return ClientConfigMetricResponse()
