import asyncio
import typing as t
from pathlib import Path

from . import group_key, local_info
from .cache_data import CacheData
from .filter import ConfigFilterChainManager
from .listener import Listener
from .push_req_handler import ConfigPushRequestHandler
from .request import *
from .response import *
from .._auth.security_proxy import SecurityProxy
from .._utils import tenant_util
from ..common import (
    conf,
    utils,
    constants as cst,
    GrpcClient,
    ConnectionEventListener,
)
from ..common.log import logger
from ..common.request import Request
from ..common.server_manager import ServerManager

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
        namespace: str,
        cache_dir: Path,
    ):
        self._server_manager = server_manager
        self._encode = conf.encode or cst.ENCODE
        self._namespace = namespace
        self._cache_dir = cache_dir
        self._listen_execute_bell = asyncio.Queue(maxsize=1)
        self._last_all_sync_time = utils.timestamp()

        labels = {
            cst.LABEL_SOURCE: cst.LABEL_SOURCE_SDK,
            cst.LABEL_MODULE: cst.LABEL_MODULE_CONFIG,
        }
        self._grpc_client = GrpcClient(CONFIG, self._server_manager, labels)
        self._chain_manager = chain_manager
        self._cache_map: t.Dict[str, CacheData] = {}
        self._security_proxy = SecurityProxy(CONFIG, server_manager.get_server_urls())
        self._redo_service = ConfigRedoService(self)
        self.listen_task: t.Optional[asyncio.Task] = None

    @property
    def cache_map(self):
        return self._cache_map

    async def start(self):
        logger.debug("[Config] client start")
        # todo start proxy when connected
        self._security_proxy.start()
        self._grpc_client.register_request_handler(
            ConfigPushRequestHandler(self.notify_listen_config, self._cache_map)
        )
        self._grpc_client.register_connection_listener(self._redo_service)
        # todo server list change event
        await self._grpc_client.start()

    def stop(self):
        logger.debug("[Config] client stop")
        self._grpc_client.stop()
        # todo stop proxy when disconnected
        self._security_proxy.stop()

    async def _req2server(self, req: Request, throw: bool = True):
        req.headers.update(self._security_proxy.get_identity_context())
        req.headers.update(self.get_common_header())
        return await self._grpc_client.request(req, throw=throw)

    def add_listeners(self, data_id: str, group: str, listeners: t.List[Listener]):
        cache = self.add_cache_data(data_id, group)

        for listener in listeners:
            cache.add_listener(listener)

        cache.is_sync_with_server = False
        self.notify_listen_config()

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
        cache = self.add_cache_data(data_id, group, self._namespace)

        for listener in listeners:
            cache.add_listener(listener)

        cache.is_sync_with_server = False

        self.notify_listen_config()

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
        cache = self.get_cache(data_id, group, self._namespace)
        if cache:
            cache.remove_listener(listener)
            if not cache.listeners:
                cache.is_sync_with_server = False
                self.notify_listen_config()

    def add_cache_data(self, data_id: str, group: str, tenant: str = ""):
        cache = self.get_cache(data_id, group, tenant)
        if cache:
            return cache

        key = group_key.get_key_tenant(data_id, group, tenant)
        cache = CacheData(
            self._chain_manager,
            self._server_manager.name,
            data_id,
            group,
            tenant=tenant,
        )
        self._cache_map[key] = cache
        return cache

    def remove_cache(self, data_id: str, group: str, tenant: str):
        key = group_key.get_key_tenant(data_id, group, tenant)
        self._cache_map.pop(key)

    def get_cache(self, data_id: str, group: str, tenant: str = ""):
        tenant = tenant or tenant_util.get_user_tenant_for_acm()
        return self._cache_map.get(group_key.get_key_tenant(data_id, group, tenant))

    def get_common_header(self):
        ts = str(utils.timestamp_milli())
        return {
            cst.CLIENT_APPNAME_HEADER: "unknown",
            cst.CLIENT_REQUEST_TS_HEADER: ts,
            cst.CLIENT_REQUEST_TOKEN_HEADER: utils.md5_hex(ts + "", cst.ENCODE),
            CONFIG_INFO_HEADER: DEFAULT_CONFIG_INFO,
            cst.CHARSET_KEY: self._encode,
        }

    async def query_config(
        self, data_id: str, group: str, tenant: str, notify: bool, throw: bool = True
    ):
        req = ConfigQueryRequest(dataId=data_id, group=group, tenant=tenant)
        req.headers.update({NOTIFY_HEADER: str(notify).lower()})

        rsp: ConfigQueryResponse = await self._req2server(req, throw=throw)
        if not rsp:
            return

        if rsp.success:
            local_info.save_snapshot(
                data_id, group, tenant, self._cache_dir, rsp.content
            )
            local_info.save_encrypt_snapshot(
                data_id, group, tenant, self._cache_dir, rsp.encryptedDataKey
            )
            rsp.contentType = rsp.contentType or "text"
            return rsp
        elif rsp.errorCode == ConfigQueryResponse.CONFIG_NOT_FOUND:
            local_info.save_snapshot(data_id, group, tenant, self._cache_dir, None)
            local_info.save_encrypt_snapshot(
                data_id, group, tenant, self._cache_dir, None
            )
            return rsp
        elif rsp.errorCode == ConfigQueryResponse.CONFIG_QUERY_CONFLICT:
            pass
        else:
            pass

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
        return bool(rsp) and rsp.success

    async def remove_config(
        self, data_id: str, group: str, tenant: str, tag: str = None
    ):
        req = ConfigRemoveRequest(data_id=data_id, group=group, tenant=tenant, tag=tag)
        rsp: ConfigRemoveResponse = await self._req2server(req)
        return rsp and rsp.success

    async def refresh_content_and_check(self, changed_key: str, notify: bool):
        # todo notify meaning
        # logger.debug("[Config] Refresh content and check md5: %s", changed_key)

        try:
            cache = self._cache_map.get(changed_key)
            if cache is None:
                return

            rsp: ConfigQueryResponse = await self.query_config(
                cache.data_id, cache.group, cache.tenant, notify, throw=False
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

    async def wait_listen_config(self):
        # logger.debug("[Config] wait listen config queue")

        while True:
            try:
                await asyncio.wait_for(self._listen_execute_bell.get(), START_INTERVAL)
            except asyncio.TimeoutError:
                # do nothing
                pass
            except asyncio.CancelledError:
                # Stop loop when cancel task.
                break
            except Exception as err:
                logger.error("[Config] wait listen config queue failed: %s", err)
                continue

            # wait until a queue item or timeout
            await self.listen_config()

    def notify_listen_config(self):
        try:
            self._listen_execute_bell.put_nowait(None)
        except asyncio.QueueFull:
            # skip
            pass
        except Exception as err:
            logger.error("[Config] notify listen config failed: %s", err)

    async def listen_config(self):
        # logger.debug("[Config] listen config")

        try:
            listen_caches: t.List[CacheData] = []
            remove_listen_caches: t.List[CacheData] = []
            now = utils.timestamp()
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
                rsp: ConfigChangeBatchListenResponse = await self._req2server(
                    req, False
                )
                if rsp and rsp.success:
                    if rsp.changedConfigs:
                        has_changed_keys = True

                    changed_keys = []
                    # Handle changed keys, notify listeners.
                    for config in rsp.changedConfigs:
                        key = group_key.get_key_tenant(
                            config.dataId, config.group, config.tenant
                        )
                        changed_keys.append(key)
                        is_initializing = self._cache_map.get(key).is_initializing
                        await self.refresh_content_and_check(key, not is_initializing)

                    # Handle no changed keys
                    for cache in listen_caches:
                        key = group_key.get_key_tenant(
                            cache.data_id, cache.group, cache.tenant
                        )
                        if key not in changed_keys:
                            if not cache.listeners:
                                continue

                            # update last modified time
                            cache.last_modified_time = utils.timestamp()
                            cache.is_sync_with_server = True

                        cache.is_initializing = False

            # Remove listen config and cache.
            if remove_listen_caches:
                req = self.wrap_config_batch_listen_request(listen_caches, False)
                rsp: ConfigChangeBatchListenResponse = await self._req2server(
                    req, False
                )
                if rsp and rsp.success:
                    for cache in remove_listen_caches:
                        if not cache.listeners:
                            self.remove_cache(cache.data_id, cache.group, cache.tenant)

            # reset last all sync time
            if need_all_sync:
                self._last_all_sync_time = now

            # re sync md5 !!!
            if has_changed_keys:
                self.notify_listen_config()
            # else:
            #     logger.debug("[Config] listen config, no changed")
        except Exception as err:
            logger.error("[Config] listen config failed: %s", err)

    @staticmethod
    def wrap_config_batch_listen_request(cache_list: t.List[CacheData], listen: bool):
        req = ConfigBatchListenRequest()
        req.listen = listen
        for cache in cache_list:
            req.configListenContexts.append(
                ConfigListenContext(
                    dataId=cache.data_id,
                    group=cache.group,
                    tenant=cache.tenant,
                    md5=cache.md5,
                )
            )
        return req

    def server_health(self):
        return self._grpc_client.is_running()


class ConfigRedoService(ConnectionEventListener):
    def __init__(self, client: ConfigClient):
        self._config_client = client
        # self._is_first_connect = True

    def on_connected(self):
        # logger.debug("[Config] on connected.")
        self._config_client.listen_task = asyncio.create_task(
            self._config_client.wait_listen_config()
        )
        self._config_client.notify_listen_config()

    def on_disconnected(self):
        # logger.debug("[Config] on disconnected.")
        if (
            self._config_client.listen_task
            and not self._config_client.listen_task.done()
        ):
            self._config_client.listen_task.cancel()
            self._config_client.listen_task = None

        for cache in self._config_client.cache_map.values():
            cache.is_sync_with_server = False
