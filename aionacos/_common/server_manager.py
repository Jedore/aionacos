from . import properties, constants as cst


class ServerManager(object):
    def __init__(self):
        server_list = properties.server_addr.split(cst.SERVER_ADDR_SPLITER)
        self._server_list = []
        self._server_urls = []
        for item in server_list:
            ip, port = item.split(cst.COLON)
            port = int(port)
            self._server_urls.append(cst.HTTP_PREFIX + item)
            self._server_list.append(
                f"{ip}{cst.COLON}{port + cst.SDK_GRPC_PORT_DEFAULT_OFFSET}"
            )

        # todo init param
        self._cur_server = None

        self.iterator = iter(self._server_list)

    def next_server(self):
        """
        Get next server address
        """

        try:
            self._cur_server = next(self.iterator)
            return self._cur_server
        except StopIteration:
            pass

        self.refresh_server()
        return self._cur_server

    def cur_server(self):
        """
        Get current server address
        """

        if not self._cur_server:
            self._cur_server = next(self.iterator)
        return self._cur_server

    def refresh_server(self):
        self.iterator = iter(self._server_list)
        self._cur_server = next(self.iterator)

    def is_server_valid(self, server_addr: str):
        return server_addr in self._server_list

    def size(self):
        return len(self._server_list)

    def get_server_urls(self):
        return self._server_urls
