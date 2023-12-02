import asyncio
import typing as t

from . import group_key
from .cache_data import CacheData
from .filter import ConfigFilterChainManager
from .listener import Listener
from .push_req_handler import ConfigPushRequestHandler
from .request import *
from .response import *
from .. import _utils
from .._auth.security_proxy import SecurityProxy
from .._common import (
    properties,
    constants as cst,
    GrpcClient,
    ConnectionEventListener,
)
from .._common.exceptions import NacosException
from .._common.log import logger
from .._common.request import Request
from .._common.response import ResponseType
from .._common.server_manager import ServerManager
from .._utils import tenant_util, md5_util

NOTIFY_HEADER = "notify"
CONFIG_INFO_HEADER = "exConfigInfo"
DEFAULT_CONFIG_INFO = "true"
ALL_SYNC_INTERVAL = 5 * 60  # seconds
START_INTERVAL = 5

TAG_PARAM = "tag"
APP_NAME_PARAM = "appName"
BETAIPS_PARAM = "betaIps"
TYPE_PARAM = "type"
ENCRYPTED_DATA_KEY_PARAM = "encryptedDataKey"
CONFIG = "Config"


class ConfigClient(object):
    def __init__(
        self,
        chain_manager: ConfigFilterChainManager,
        server_manager: ServerManager,
    ):
        self._encode = properties.encode or cst.ENCODE
        self._tenant = properties.config_namespace
        # todo security proxy

        self._listen_execute_bell = asyncio.Queue(maxsize=1)
        self._last_all_sync_time = _utils.timestamp()

        labels = {
            cst.LABEL_SOURCE: cst.LABEL_SOURCE_SDK,
            cst.LABEL_MODULE: cst.LABEL_MODULE_CONFIG,
        }
        self._grpc = GrpcClient(CONFIG, server_manager, labels)
        self._chain_manager = chain_manager
        self._cache_map: t.Dict[str, CacheData] = {}
        self._security_proxy = SecurityProxy(CONFIG, server_manager.get_server_urls())
        self._redo_service = ConfigRedoService(self)
        self._listen_task: t.Optional[asyncio.Task] = None

    @property
    def cache_map(self):
        return self._cache_map

    async def start(self):
        logger.debug("[Config] client start")
        self._security_proxy.refresh_auth_task()
        self._grpc.reg_req_handler(
            ConfigPushRequestHandler(self.notify_listen_config, self._cache_map)
        )
        self._grpc.reg_conn_listener(ConfigConnectionListener(self))
        # todo server list change event
        await self._grpc.start()
        await asyncio.sleep(3)
        self._listen_task = asyncio.create_task(self.listen_config_loop())

    def stop(self):
        logger.debug("[Config] client stop")
        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()
        self._grpc.stop()
        self._security_proxy.stop()

    async def _req2server(self, req: Request, throw: bool = True) -> ResponseType:
        req.headers.update(self._security_proxy.get_identity_context())
        rsp: ResponseType = await self._grpc.request(req, throw=throw)
        if rsp.success:
            return rsp

        # when request failed
        logger.error("[Config] req failed: %s, %s", req, rsp)
        if throw:
            NacosException(rsp.errorCode, rsp.message)

    def add_listeners(self, data_id: str, group: str, listeners: t.List[Listener]):
        cache = self.add_cache_data_if_absent(data_id, group)

        for listener in listeners:
            cache.add_listener(listener)

        cache.is_sync_with_server = False
        self.notify_listen_config()

        # todo
        self._redo_service.cache(self.add_listeners, data_id, group, listeners)

    def remove_listener(self, data_id: str, group: str, listener: Listener):
        cache = self.get_cache(data_id, group)
        if cache:
            cache.remove_listener(listener)
            if not cache.listeners:
                cache.is_sync_with_server = False

                self.notify_listen_config()

    def add_tenant_listeners(
        self, data_id: str, group: str, listeners: t.List[Listener]
    ):
        cache = self.add_cache_data_if_absent(data_id, group, self._tenant)

        for listener in listeners:
            cache.add_listener(listener)

        cache.is_sync_with_server = False

        self.notify_listen_config()

        # todo
        self._redo_service.cache(self.add_tenant_listeners, data_id, group, listeners)

    def add_tenant_listeners_with_content(
        self,
        data_id: str,
        group: str,
        content: str,
        encrypted_data_key: str,
        listeners: t.List[Listener],
    ):
        # todo
        pass

    def remove_tenant_listener(self, data_id: str, group: str, listener: Listener):
        cache = self.get_cache(data_id, group, self._tenant)
        if cache:
            cache.remove_listener(listener)
            if not cache.listeners:
                cache.is_sync_with_server = False

                self.notify_listen_config()

    def add_cache_data_if_absent(self, data_id: str, group: str, tenant: str = ""):
        cache = self.get_cache(data_id, group, tenant)
        if cache:
            return cache

        key = group_key.get_key_tenant(data_id, group, tenant)
        cache = CacheData(self._chain_manager, "name", data_id, group, tenant=tenant)

        self._cache_map[key] = self._cache_map.get(key) or cache

        return cache

    def remove_cache(self, data_id: str, group: str, tenant: str):
        key = group_key.get_key_tenant(data_id, group, tenant)
        self._cache_map.pop(key)

    def get_cache(self, data_id: str, group: str, tenant: str = ""):
        tenant = tenant or tenant_util.get_user_tenant_for_acm()
        return self._cache_map.get(group_key.get_key_tenant(data_id, group, tenant))

    def get_common_header(self):
        ts = str(_utils.timestamp_milli())
        return {
            cst.CLIENT_APPNAME_HEADER: "unknown",
            cst.CLIENT_REQUEST_TS_HEADER: ts,
            cst.CLIENT_REQUEST_TOKEN_HEADER: md5_util.md5_hex(ts + "", cst.ENCODE),
            CONFIG_INFO_HEADER: DEFAULT_CONFIG_INFO,
            cst.CHARSET_KEY: self._encode,
        }

    async def query_config(self, data_id: str, group: str, tenant: str, notify: bool):
        req = ConfigQueryRequest(dataId=data_id, group=group, tenant=tenant)
        req.headers.update({NOTIFY_HEADER: str(notify).lower()})
        req.headers.update(self.get_common_header())

        rsp: ConfigQueryResponse = await self._req2server(req)
        if rsp.success:
            rsp.contentType = rsp.contentType or "text"
            # todo save snapshot encrypted
            return rsp
        elif rsp.errorCode == ConfigQueryResponse.CONFIG_NOT_FOUND:
            # todo save snapshot encrypted
            pass
        elif rsp.errorCode == ConfigQueryResponse.CONFIG_QUERY_CONFLICT:
            # todo CONFIG_QUERY_CONFLICT
            pass
        else:
            # todo other error
            pass

        logger.error("[Config] query config failed: %s, %s", rsp, req)
        raise NacosException(rsp.errorCode, msg=rsp.message)

    async def publish_config(
        self,
        data_id: str,
        group: str,
        tenant: str,
        content: str,
        encrypted_data_key: str,
        type_: str,
        app_name: str = None,
        tag: str = None,
        beta_ips: str = None,
        cas_md5: str = None,
    ):
        req = ConfigPublishRequest(
            data_id=data_id,
            group=group,
            tenant=tenant,
            content=content,
            cas_md5=cas_md5,
        )
        req.additionMap.update(
            {
                TAG_PARAM: tag,
                APP_NAME_PARAM: app_name,
                BETAIPS_PARAM: beta_ips,
                TYPE_PARAM: type_,
                ENCRYPTED_DATA_KEY_PARAM: encrypted_data_key or "",
            }
        )
        rsp: ConfigPublishResponse = await self._req2server(req)
        return rsp.success

    async def remove_config(
        self, data_id: str, group: str, tenant: str, tag: str = None
    ) -> bool:
        req = ConfigRemoveRequest(data_id=data_id, group=group, tenant=tenant, tag=tag)
        rsp: ConfigRemoveResponse = await self._req2server(req)
        return rsp.success

    async def refresh_content_and_check(self, changed_key: str, notify: bool):
        """

        notify: no use currently.
        """
        logger.debug("[Config] Refresh content and check md5: %s", changed_key)

        try:
            cache = self._cache_map.get(changed_key)
            if cache is None:
                return

            rsp: ConfigQueryResponse = await self.query_config(
                cache.data_id, cache.group, cache.tenant, notify
            )

            # Push empty protection.
            # if not rsp.content:
            #     if not properties.update_cache_when_empty:
            #         return

            cache.encrypted_data_key = rsp.encryptedDataKey
            cache.set_content(rsp.content)
            cache.type = rsp.contentType or cache.type

            cache.check_listener_md5()
        except Exception as err:
            logger.error(
                "[Config] refresh content and check md5 failed: %s, %s",
                changed_key,
                err,
            )

    async def listen_config_loop(self):
        """
        Read notification from _listen_execute_bell queue,
        then listen config change when server request or exceed fixed time.
        """
        logger.info("[Config] start listen config")

        while True:
            try:
                await asyncio.wait_for(self._listen_execute_bell.get(), START_INTERVAL)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                # Stop loop when cancel task.
                break
            except Exception as err:
                logger.error("[Config] get _listen_execute_bell queue failed: %s", err)
                continue

            # get _listen_execute_bell queue success, then listen config.
            await self.listen_config()

    def notify_listen_config(self):
        try:
            self._listen_execute_bell.put_nowait(None)
        except asyncio.QueueFull:
            # do nothing
            pass
        except Exception as err:
            logger.error("[Config] put _listen_execute_bell queue failed: %s", err)

    async def listen_config(self):
        logger.debug("[Config] listen config")

        try:
            listen_caches: t.List[CacheData] = []
            remove_listen_caches: t.List[CacheData] = []
            now = _utils.timestamp()
            need_all_sync = now - self._last_all_sync_time >= ALL_SYNC_INTERVAL
            for cache in self._cache_map.values():
                if cache.is_sync_with_server:
                    cache.check_listener_md5()
                    if not need_all_sync:
                        continue

                if cache.listeners:
                    if not cache.is_use_local_config:
                        listen_caches.append(cache)
                else:
                    if not cache.is_use_local_config:
                        remove_listen_caches.append(cache)

            has_changed_keys = False

            # Add listen config and cache.
            if listen_caches:
                req = self.wrap_config_batch_listen_request(listen_caches, True)
                rsp: ConfigChangeBatchListenResponse = await self._req2server(req)
                if rsp.success:
                    if rsp.changedConfigs:
                        has_changed_keys = True

                    changed_keys = []

                    for config in rsp.changedConfigs:
                        key = group_key.get_key_tenant(
                            config.dataId, config.group, config.tenant
                        )

                        changed_keys.append(key)

                        is_initializing = self._cache_map.get(key).is_initializing

                        await self.refresh_content_and_check(key, not is_initializing)

                    for cache in listen_caches:
                        key = group_key.get_key_tenant(
                            cache.data_id, cache.group, cache.tenant
                        )
                        if key not in changed_keys:
                            if not cache.listeners:
                                continue

                            if cache.last_modified_time == _utils.timestamp():
                                cache.is_sync_with_server = True

                        cache.is_initializing = False
                else:
                    logger.error("[Config] Listen cache failed: %s", rsp)

            # Remove listen config and cache.
            if remove_listen_caches:
                req = self.wrap_config_batch_listen_request(listen_caches, False)
                rsp: ConfigChangeBatchListenResponse = await self._req2server(req)
                if rsp.success:
                    for cache in remove_listen_caches:
                        if not cache.listeners:
                            self.remove_cache(cache.data_id, cache.group, cache.tenant)
                else:
                    logger.error("[Config] remove listen cache failed: %s", rsp)

            if need_all_sync:
                self._last_all_sync_time = now

            if has_changed_keys:
                self.notify_listen_config()
            else:
                logger.debug("[Config] listen config, no changed")
        except Exception as err:
            logger.error("[Config] listen config failed: %s", err)

    @staticmethod
    def wrap_config_batch_listen_request(cache_list: t.List[CacheData], listen: bool):
        req = ConfigBatchListenRequest()
        req.listen = listen
        for cache in cache_list:
            req.configListenContexts.append(
                ConfigListenContext(cache.data_id, cache.group, cache.md5, cache.tenant)
            )
        return req

    def server_health(self):
        return self._grpc.is_running()


class ConfigConnectionListener(ConnectionEventListener):
    def __init__(self, client: ConfigClient):
        self._client = client


class ConfigRedoService(ConnectionEventListener):
    def __init__(self, client: ConfigClient):
        self._client = client
        self._is_first_connected = True

    def on_connected(self):
        self._client.notify_listen_config()
        asyncio.create_task(self.run())
        logger.debug("[Config] on connected.")

    def on_disconnected(self):
        for cache in self._client.cache_map.values():
            cache.is_sync_with_server = False
        logger.debug("[Config] on disconnected.")

    async def run(self):
        # Do not redo when connect firstly.
        if self._is_first_connected:
            self._is_first_connected = False
            return

        logger.debug("[Config] redo service start")
