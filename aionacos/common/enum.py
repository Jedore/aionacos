from enum import Enum
from types import DynamicClassAttribute


class EnumBase(Enum):
    @DynamicClassAttribute
    def code(self):
        return self._value_[0]

    @DynamicClassAttribute
    def desc(self):
        return self._value_[1]
