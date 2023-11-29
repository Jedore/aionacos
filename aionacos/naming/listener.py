from abc import ABCMeta

from .._common.event import Event


class EventListener(metaclass=ABCMeta):
    def on_event(self, event: Event):
        raise NotImplementedError
