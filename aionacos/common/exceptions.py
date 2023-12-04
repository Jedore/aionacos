class NacosException(Exception):
    __slots__ = ("_code", "_msg")  # _ms

    CLIENT_INVALID_PARAM = -400  # invalid param.
    CLIENT_DISCONNECT = -401  # client disconnect
    CLIENT_OVER_THRESHOLD = -503  # over client threshold
    CLIENT_ERR = -500  # client error

    INVALID_PARAM = 400  # invalid param
    NO_RIGHT = 403  # no right
    NOT_FOUND = 404  # no found
    CONFLICT = 409  # conflict
    SERVER_ERR = 500  # server error
    BAD_GATEWAY = 502  # bad gateway
    OVER_THRESHOLD = -503  # over server threshold
    INVALID_SERVER_STATUS = -300  # server is not started
    UN_REGISTER = 301  # connection is not registered
    NO_HANDLER = 302  # no handler found

    def __init__(self, code: int, msg: str = ""):
        # todo according Exception
        self._code = code
        self._msg = msg

    def __str__(self):
        return f"code: {self._code}, msg: {self._msg}"

    @property
    def msg(self):
        return self._msg


class RuntimeException(NacosException):
    ...


class NSRuntimeException(RuntimeException):
    ...


class RemoteException(NSRuntimeException):
    ...
