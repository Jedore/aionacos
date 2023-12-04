from pathlib import Path

from . import local_info
from .client import ConfigClient
from .filter import ConfigFilterChainManager, ConfigFilter
from .listener import Listener
from .response import *
from ..common import conf, constants as cst
from ..common.exceptions import NacosException
from ..common.log import logger
from ..common.server_manager import ServerManager


class ConfigService(object):
    def __init__(self, namespace: str = None):
        self._namespace = namespace or conf.config_namespace
        # todo filter chain
        self._filter_chain_manager = ConfigFilterChainManager()
        self._cache_dir = conf.cache_dir / "config"
        server_manager = ServerManager()
        # todo server list update
        self._client = ConfigClient(
            self._filter_chain_manager,
            server_manager,
            self._namespace,
            self._cache_dir,
        )

    async def start(self):
        logger.info("[Config] service start")
        await self._client.start()

    def stop(self):
        logger.info("[Config] service stop")
        self._client.stop()

    async def get_config(self, data_id: str, group: str = cst.DEFAULT_GROUP):
        content = await local_info.get_snapshot(
            data_id, group, self._namespace, self._cache_dir
        )
        if content is not None:
            return content

        try:
            rsp: ConfigQueryResponse = await self._client.query_config(
                data_id, group, self._namespace, False
            )
            # todo filter
            return rsp.content
        except NacosException as err:
            pass

    async def get_config_and_sign_listener(
        self, data_id: str, listener: Listener, group: str = cst.DEFAULT_GROUP
    ):
        # todo
        pass

    async def publish_config(
        self, data_id: str, content: str, type_: str, group: str = cst.DEFAULT_GROUP
    ):
        # todo encrypt and publish config
        pass

    async def publish_config_cas(
        self,
        data_id: str,
        content: str,
        cas_md5: str,
        type_: str,
        group: str = cst.DEFAULT_GROUP,
    ):
        pass

    async def remove_config(self, data_id: str, group: str = cst.DEFAULT_GROUP) -> bool:
        return await self._client.remove_config(data_id, group, self._namespace)

    def add_listener(
        self, data_id: str, listener: Listener, group: str = cst.DEFAULT_GROUP
    ):
        self._client.add_tenant_listeners(data_id, group, [listener])

    def remove_listener(
        self, data_id: str, listener: Listener, group: str = cst.DEFAULT_GROUP
    ):
        self._client.remove_tenant_listener(data_id, group, listener)

    def add_config_filter(self, filter_: ConfigFilter):
        pass

    def get_server_status(self):
        return "UP" if self._client.server_health() else "DOWN"
