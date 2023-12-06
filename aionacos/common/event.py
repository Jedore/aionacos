from .payload import SlotsMeta


class Event(metaclass=SlotsMeta):
    def __init__(self):
        self.serialVersionUI = -3731383194964997493
        # todo event sequence
        self.sequence = 1

    def scope(self):
        pass


class SlowEvent(Event):
    pass
