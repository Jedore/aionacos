import asyncio
from typing import List, Optional

from .plugin_manager import AuthPluginManager
from .._common import properties
from .._common.log import logger


class SecurityProxy(object):
    _is_refreshing = False
    _ref_count = 0

    def __init__(self, name: str, server_urls: List[str]):
        self._ref_count += 1
        self._name = name
        self._task: Optional[asyncio.Task] = None
        self._auth_plugin_manager = AuthPluginManager(name, server_urls)

    def login(self):
        """
        Login all auth services in Java client, but I think one is ok.
        """
        for auth_service in self._auth_plugin_manager.auth_services:
            if auth_service.login():
                return

    def get_identity_context(self) -> dict:
        """
        Use identity context of all auth services in Java client, but I think one is ok.
        """
        # headers = {}
        for auth_service in self._auth_plugin_manager.auth_services:
            if auth_service.identity_context:
                # headers.update(auth_service.identity_context)
                return auth_service.identity_context
        # return headers
        return {}

    def refresh_auth_task(self):
        if not properties.auth_enable:
            logger.warning("[%s] auth disabled", self._name)
            return

        if self._is_refreshing:
            return

        self._is_refreshing = True

        if not self._auth_plugin_manager.auth_services:
            logger.warning("[%s] no auth services", self._name)
            return

        async def loop():
            logger.debug("[%s] start refresh auth status", self._name)

            while True:
                try:
                    self.login()
                    await asyncio.sleep(5)
                except asyncio.CancelledError:
                    # Stop loop when cancel task.
                    break

        self._task = asyncio.create_task(loop())

    def stop(self):
        self._ref_count -= 1
        if self._ref_count <= 0:
            if self._task and not self._task.done():
                self._task.cancel()
