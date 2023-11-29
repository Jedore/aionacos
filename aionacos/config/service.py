from .._common import properties, constants as cst
from .._common.exceptions import NacosException
from .._common.log import logger
from .._common.server_manager import ServerManager
from ..config.client import ConfigClient
from ..config.filter import ConfigFilterChainManager, ConfigFilter
from ..config.listener import Listener
from ..config.local_info_processor import LocalConfigInfoProcessor


class ConfigService(object):
    def __init__(self):
        self._filter_chain_manager = None
        # todo namespace
        self._namespace = properties.config_namespace

        self._filter_chain_manager = ConfigFilterChainManager()

        server_manager = ServerManager()
        # todo server update; ignore

        self._client = ConfigClient(self._filter_chain_manager, server_manager)

    async def start(self):
        logger.info("[Config] Service start ...")
        await self._client.start()

    def stop(self):
        logger.info("[Config] Service stop ...")
        self._client.stop()

    async def get_config(
        self,
        data_id: str,
        group: str = cst.DEFAULT_GROUP,
    ):
        # todo
        content = LocalConfigInfoProcessor.get_failover()
        if content is not None:
            return content

        try:
            rsp = await self._client.query_config(
                data_id, group, self._namespace, False
            )
            # todo filter
            return rsp.content
        except NacosException as e:
            pass

    async def get_config_and_sign_listener(
        self,
        data_id: str,
        listener: Listener,
        group: str = cst.DEFAULT_GROUP,
    ):
        # todo
        pass

    async def publish_config(
        self,
        data_id: str,
        content: str,
        type_: str,
        group: str = cst.DEFAULT_GROUP,
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

    async def remove_config(
        self,
        data_id: str,
        group: str = cst.DEFAULT_GROUP,
    ) -> bool:
        return await self._client.remove_config(data_id, group, self._namespace)

    def add_listener(
        self,
        data_id: str,
        listener: Listener,
        group: str = cst.DEFAULT_GROUP,
    ):
        self._client.add_tenant_listeners(data_id, group, [listener])

    def remove_listener(
        self,
        data_id: str,
        listener: Listener,
        group: str = cst.DEFAULT_GROUP,
    ):
        self._client.remove_tenant_listener(data_id, group, listener)

    def get_server_status(self):
        return "UP" if self._client.server_health() else "DOWN"

    def add_config_filter(self, filter_: ConfigFilter):
        pass
