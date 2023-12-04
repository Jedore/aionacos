import enum

from ..common.event import SlowEvent
from ..common.utils import Serializable


class ChangeType(enum.IntEnum):
    ADDED = 1
    MODIFIED = 2
    DELETED = 3


class ConfigChangeItem(Serializable):
    __slots__ = ("key", "old_value", "new_value", "type")

    def __init__(
        self,
        key: str = None,
        old_value: str = None,
        new_value: str = None,
        type_: ChangeType = None,
    ):
        self.key = key
        self.old_value = old_value
        self.new_value = new_value
        self.type = type_


class ConfigChangeEvent(Serializable):
    __slots__ = ("data",)

    def __init__(self, data: dict):
        self.data = data


class ServerListChangeEvent(SlowEvent):
    pass
