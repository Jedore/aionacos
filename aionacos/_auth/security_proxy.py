import asyncio
from typing import List, Optional

from .plugin_manager import AuthPluginManager
from ..common import conf
from ..common.log import logger


class SecurityProxy(object):
    _is_refreshing = False
    _ref_count = 0

    def __init__(self, service: str, server_urls: List[str]):
        self._ref_count += 1
        self._service = service
        self._task: Optional[asyncio.Task] = None
        self._auth_plugin_manager = AuthPluginManager(service, server_urls)

    async def login(self):
        """
        Login all auth services in Java client, but I think one is ok.
        """
        for auth_service in self._auth_plugin_manager.auth_services:
            if await auth_service.login():
                return

    def get_identity_context(self) -> dict:
        # todo auth enable
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

    def start(self):
        logger.debug("[%s] security proxy start", self._service)

        if not conf.auth_enable:
            logger.warning("[%s] auth disabled", self._service)
            return

        if self._is_refreshing:
            return

        self._is_refreshing = True

        if not self._auth_plugin_manager.auth_services:
            logger.warning("[%s] no auth services", self._service)
            return

        async def loop():
            while True:
                try:
                    await self.login()
                    await asyncio.sleep(5)
                except asyncio.CancelledError:
                    # Stop loop when cancel task.
                    break
                except Exception as err:
                    logger.exception("[%s] auth error: %s", self._service, err)

        self._task = asyncio.create_task(loop())

    def stop(self):
        logger.debug("[%s] security proxy stop", self._service)

        self._ref_count -= 1
        if self._ref_count <= 0:
            if self._task and not self._task.done():
                self._task.cancel()
                self._task = None

    def auth(self):
        pass

    # todo auth succeeded then request
