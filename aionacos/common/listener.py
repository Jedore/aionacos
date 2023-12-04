from abc import ABCMeta


class ConnectionEventListener(metaclass=ABCMeta):
    def on_connected(self):
        raise NotImplementedError()

    def on_disconnected(self):
        raise NotImplementedError()

    @property
    def name(self):
        return self.__class__.__name__
