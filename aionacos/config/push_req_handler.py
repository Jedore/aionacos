import typing as t

from . import group_key
from .cache_data import CacheData
from .request import *
from .response import *
from .. import _utils
from ..common.request import Request
from ..common.server_req_handler import ServerRequestHandler


class ConfigPushRequestHandler(ServerRequestHandler):
    def __init__(
        self, notify_listen_config: t.Callable, cache_map: t.Dict[str, CacheData]
    ):
        self._notify_listen_config = notify_listen_config
        self._cache_map = cache_map

    async def request_reply(self, req: Request):
        if isinstance(req, ConfigChangeNotifyRequest):
            key = group_key.get_key_tenant(req.dataId, req.group, req.tenant)
            cache = self._cache_map.get(key)
            if cache:
                cache.last_modified_time = _utils.timestamp()
                cache.is_sync_with_server = False
                self._notify_listen_config()
            return ConfigChangeNotifyResponse()
        elif isinstance(req, ClientConfigMetricRequest):
            # todo metric request
            return ClientConfigMetricResponse()
