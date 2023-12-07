import abc
import typing as t

import aiohttp

from . import constants as const
from ..common import conf, utils
from ..common.log import logger


class AbstractAuthService(abc.ABC):
    identity_context: t.Optional[dict] = None
    server_urls: t.Optional[list] = None

    def __init__(self, service: str):
        self._service = service

    @abc.abstractmethod
    async def login(self):
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


class HttpAuthService(AbstractAuthService):
    _token_ttl = 0
    _token_refresh_window = 0
    _last_refresh_time = 0
    identity_context = {}

    def __init__(self, service: str):
        super().__init__(service)
        self._session: t.Optional[aiohttp.ClientSession] = None

    async def login(self):
        if not self._session:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=conf.login_timeout)
            )

        # logger.debug("[%s] %s login check", self._service, self)

        # Check whether identity is expired.
        if (
            utils.timestamp() - self._last_refresh_time
            < self._token_ttl - self._token_refresh_window
        ):
            return True

        for url in self.server_urls:
            try:
                async with self._session.post(
                    url + conf.login_path,
                    params={"username": conf.username},
                    data={"password": conf.password},
                ) as rsp:
                    if rsp.status == 200:
                        data = await rsp.json()
                        self.identity_context[const.ACCESSTOKEN] = data.get(
                            const.ACCESSTOKEN
                        )
                        self._token_ttl = data.get(const.TOKENTTL)
                        self._token_refresh_window = self._token_ttl / 10
                        self._last_refresh_time = utils.timestamp()

                        logger.info(
                            "[%s] %s login %s succeed", self._service, self, url
                        )
                        return True
                    else:
                        logger.error(
                            "[%s] %s login failed: %s, %s",
                            self._service,
                            self,
                            url,
                            await rsp.text(),
                        )
            except Exception as err:
                logger.error(
                    "[%s] %s login failed: %s, %s", self._service, self, url, err
                )


class RamAuthService(AbstractAuthService):
    # todo ram auth
    def login(self):
        pass

    def validate(self):
        pass
