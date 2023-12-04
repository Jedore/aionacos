from abc import ABCMeta
from typing import Optional

import httpx

from . import constants as const
from .._utils import timestamp
from ..common import conf
from ..common.log import logger


class AuthService(metaclass=ABCMeta):
    identity_context: Optional[dict] = None
    server_urls: Optional[list] = None

    def __init__(self, name: str):
        self._name = name

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
        logger.debug("[%s] %s login check", self._name, self)

        # Check whether identity is expired.
        if (
            timestamp() - self._last_refresh_time
            < self._token_ttl - self._token_refresh_window
        ):
            return True

        login_path = "/nacos/v1/auth/users/login"
        for url in self.server_urls:
            try:
                # todo use aiohttp
                rsp = httpx.post(
                    url + login_path,
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

                        logger.info("[%s] %s login %s succeed", self._name, self, url)
                        return True
                    error = rsp.text
                else:
                    error = "unknown error"
            except Exception as err:
                error = err

            logger.error("[%s] %s login failed: %s, %s", self._name, self, url, error)
