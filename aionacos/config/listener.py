from abc import ABCMeta

from .event import ConfigChangeEvent


class Listener(metaclass=ABCMeta):
    def receive_config_info(self, config_info: str):
        raise NotImplementedError()


class AbstractListener(Listener, metaclass=ABCMeta):
    pass


class AbstractSharedListener(Listener, metaclass=ABCMeta):
    pass


class PropertiesListener(AbstractListener, metaclass=ABCMeta):
    def receive_config_info(self, config_info: str):
        if not config_info:
            return
        # todo


class AbstractConfigChangeListener(AbstractListener, metaclass=ABCMeta):
    def receive_config_change(self, event: ConfigChangeEvent):
        raise NotImplementedError

    def receive_config_info(self, config_info: str):
        pass
