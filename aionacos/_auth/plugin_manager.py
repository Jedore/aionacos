from typing import Tuple, List

from .service import AuthService, NacosAuthService


class AuthPluginManager(object):

    def __init__(self, server_urls: List[str]):
        self._auth_services: Tuple[AuthService] = (NacosAuthService(),)

        for auth_service in self._auth_services:
            auth_service.server_urls = server_urls

    @property
    def auth_services(self):
        return self._auth_services
