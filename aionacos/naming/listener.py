from abc import ABCMeta

from ..common.event import Event


class EventListener(metaclass=ABCMeta):
    def on_event(self, event: Event):
        raise NotImplementedError
