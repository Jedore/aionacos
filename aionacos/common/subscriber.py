from .event import Event
from .payload import SlotsMeta


class Subscriber(metaclass=SlotsMeta):
    def on_event(self, event: Event):
        raise NotImplementedError()

    @staticmethod
    def subscribe_type():
        raise NotImplementedError()

    @staticmethod
    def ignore_expire_event() -> bool:
        return False

    def scope_matches(self, event: Event) -> bool:
        return event.scope() is None


class SmartSubscriber(Subscriber):
    @staticmethod
    def subscribe_type():
        return

    @staticmethod
    def subscribe_types():
        raise NotImplementedError()
