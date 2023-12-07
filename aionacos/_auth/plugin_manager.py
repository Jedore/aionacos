from .service import HttpAuthService, RamAuthService


class AuthPluginManager(object):
    def __init__(self, service: str, server_urls: list):
        self._auth_services = (HttpAuthService(service), RamAuthService(service))

        for auth_service in self._auth_services:
            auth_service.server_urls = server_urls

    @property
    def auth_services(self):
        return self._auth_services
