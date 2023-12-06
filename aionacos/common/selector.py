from .payload import SlotsMeta


class AbstractSelector(metaclass=SlotsMeta):
    def __init__(self, type_: str):
        self.type = type_
