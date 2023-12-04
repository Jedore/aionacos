import asyncio
import typing as t
from os import getenv

from .change_handler import ConfigChangeHandler
from .event import ConfigChangeEvent
from .filter import ConfigFilterChainManager, ConfigResponse
from .listener import (
    Listener,
    AbstractSharedListener,
    AbstractConfigChangeListener,
)
from ..common import constants as const
from ..common.exceptions import NacosException
from ..common.log import logger
from .._utils import md5_util, timestamp


class ListenerWarp(object):
    def __init__(self, listener: Listener, md5: str = None, last_content: str = None):
        self.in_notifying = False
        self.listener = listener
        self.last_call_md5 = md5
        self.last_content = last_content

    def __eq__(self, other: "ListenerWarp"):
        return self.listener == other.listener


class CacheData(object):
    def __init__(
        self,
        chain_manager: ConfigFilterChainManager,
        name: str,
        data_id: str,
        group: str,
        tenant: str = "",
    ):
        self.chain_manager = chain_manager
        self.name = name
        self.data_id = data_id
        self.group = group
        self.tenant = tenant
        self.listeners: t.List[ListenerWarp] = []
        self.type = ""
        self.last_modified_time = timestamp()  # second

        # 1.first add listener, default is false; need to check.
        # 2.receive config change notify, set false; need to check.
        # 3.last listener is remove, set to false; need to check
        self.is_sync_with_server = False

        # do not notify when True
        self.is_initializing = True

        self.content = None
        self.md5 = None

        # todo
        self._init_snapshot = getenv("nacos.cache.data.init.snapshot", "true") == "true"
        if self._init_snapshot:
            self.content = self.load_cache_data_from_disk()
            self.encrypted_data_key = self.load_encrypted_data_key_from_disk()
            self.md5 = self.get_md5()

        self.is_use_local_config = False

        # todo init snapshot

    def set_content(self, content: str):
        self.content = content
        self.md5 = self.get_md5()

    def add_listener(self, listener: Listener):
        if listener is None:
            raise NacosException(NacosException.INVALID_PARAM, msg="listener is none")

        if isinstance(listener, AbstractConfigChangeListener):
            # todo
            wrap = ListenerWarp(listener, self.md5)
        else:
            wrap = ListenerWarp(listener, self.md5)

        self.listeners.append(wrap)

    def remove_listener(self, listener: Listener):
        if listener is None:
            raise NacosException(NacosException.INVALID_PARAM, msg="listener is none")
        wrap = ListenerWarp(listener)
        try:
            self.listeners.remove(wrap)
        except ValueError:
            pass

    def check_listener_md5(self):
        """
        Check listener's md5 and cache's md5.
        """
        for wrap in self.listeners:  # type
            if self.md5 != wrap.last_call_md5:
                # if not same, notify listener
                self._notify_listener(
                    self.data_id,
                    self.group,
                    self.content,
                    self.type,
                    self.md5,
                    self.encrypted_data_key,
                    wrap,
                )

    def check_listener_md5_consistent(self):
        for wrap in self.listeners:
            if self.md5 != wrap.last_call_md5:
                return False
        return True

    @staticmethod
    def _notify_listener(
        data_id: str,
        group: str,
        content: str,
        type_: str,
        md5: str,
        encrypted_data_key: str,
        wrap: ListenerWarp,
    ):
        listener = wrap.listener
        if wrap.in_notifying:
            return

        async def _notify():
            logger.debug("[Config] Notify listeners changed configs")
            try:
                if isinstance(listener, AbstractSharedListener):
                    # todo shared listener
                    pass

                rsp = ConfigResponse(
                    dataId=data_id,
                    group=group,
                    content=content,
                    encryptedDataKey=encrypted_data_key,
                )
                # todo filter
                wrap.in_notifying = True
                listener.receive_config_info(rsp.content)
                if isinstance(listener, AbstractConfigChangeListener):
                    data = ConfigChangeHandler.parse_change_data(
                        wrap.last_content, content, type_
                    )
                    if data:
                        event = ConfigChangeEvent(data)
                        listener.receive_config_change(event)
                        wrap.last_content = content

                wrap.last_call_md5 = md5
            except Exception as e:
                logger.error("[Config] Notify listeners changed config failed: %s", e)
            finally:
                # todo
                wrap.in_notifying = False

        asyncio.create_task(_notify())

    def load_cache_data_from_disk(self):
        # todo
        pass

    def load_encrypted_data_key_from_disk(self):
        # todo
        pass

    def get_md5(self):
        return md5_util.md5_hex(self.content, const.ENCODE)
