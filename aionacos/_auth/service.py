from abc import ABCMeta
from typing import Optional

import httpx

from . import constants as const
from .._common import properties
from aionacos._common.log import logger
from .._utils import timestamp


class AuthService(metaclass=ABCMeta):
    identity_context: Optional[dict] = None
    server_urls: Optional[list] = None

    def login(self):
        raise NotImplementedError()

    # def set_request_template(self):
    #     raise NotImplementedError()
    #
    # def get_login_identity_context(self) -> dict:
    #     raise NotImplementedError()

    def __repr__(self):
        return self.__class__.__name__

    def __str__(self):
        return self.__class__.__name__


class NacosAuthService(AuthService):
    _token_ttl = 0
    _token_refresh_window = 0
    _last_refresh_time = 0
    identity_context = {}

    def login(self):
        logger.debug("[Auth] %s Login check.", self)

        # Check whether identity is expired.
        if (
            timestamp() - self._last_refresh_time
            < self._token_ttl - self._token_refresh_window
        ):
            return True

        login_path = "/nacos/v1/auth/users/login"
        for server in self.server_urls:
            try:
                rsp = httpx.post(
                    server + login_path,
                    params={"username": properties.username},
                    data={"password": properties.password},
                )

                error = None
                if isinstance(rsp, httpx.Response):
                    if rsp.status_code == 200:
                        data = rsp.json()
                        self.identity_context[const.ACCESSTOKEN] = data.get(
                            const.ACCESSTOKEN
                        )
                        self._token_ttl = data.get(const.TOKENTTL)
                        self._token_refresh_window = self._token_ttl / 10
                        self._last_refresh_time = timestamp()

                        logger.info(
                            "[Auth] %s Login %s succeed.", self, server
                        )
                        return True
                    error = rsp.text
                else:
                    error = "unknown error."
            except Exception as err:
                error = err

            logger.error("[Auth] %s Login %s failed: %s", self, server, error)
