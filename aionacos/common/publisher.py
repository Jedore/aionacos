import asyncio
from abc import ABCMeta

from .event import Event
from .log import logger
from .subscriber import Subscriber


class EventPublisher(metaclass=ABCMeta):
    def init(self, event_type: type, buffer_size: int):
        raise NotImplementedError()

    def current_event_size(self):
        raise NotImplementedError()

    def add_subscriber(self, subscriber: Subscriber):
        raise NotImplementedError()

    def remove_subscriber(self, subscriber: Subscriber):
        raise NotImplementedError()

    def publish(self, event: Event):
        raise NotImplementedError()

    def notify_subscriber(self, subscriber: Subscriber, event: Event):
        raise NotImplementedError()


class SharedEventPublisher(EventPublisher, metaclass=ABCMeta):
    pass


class DefaultPublisher(EventPublisher):
    def __init__(self):
        self.initialized = False
        self.shutdown = False
        self.queue_max_size = -1
        self.last_event_sequence = -1
        self._subscribers = set()

    def init(self, event_type: type, buffer_size: int):
        self.event_type = event_type
        self.queue_max_size = buffer_size
        self.name = "nacos.publisher-" + event_type.__name__
        self._queue = asyncio.Queue(buffer_size)

        asyncio.create_task(self.open_event_handler())

    def add_subscriber(self, subscriber: Subscriber):
        self._subscribers.add(subscriber)

    def remove_subscriber(self, subscriber: Subscriber):
        if subscriber in self._subscribers:
            self._subscribers.remove(subscriber)

    def has_subscriber(self):
        return bool(self._subscribers)

    def current_event_size(self):
        return self._queue.qsize()

    async def open_event_handler(self):
        while True:
            event = await self._queue.get()
            self.receive_event(event)
            self.last_event_sequence = max(
                self.last_event_sequence, event.sequence
            )

    def publish(self, event: Event):
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            pass

    def receive_event(self, event: Event):
        if not self.has_subscriber():
            return

        for subscriber in self._subscribers:
            if not subscriber.scope_matches(event):
                continue

            if (
                subscriber.ignore_expire_event()
                and self.last_event_sequence > event.sequence
            ):
                logger.debug("[NotifyCenter] expired event: %s", event)
                continue

            self.notify_subscriber(subscriber, event)

    def notify_subscriber(self, subscriber: Subscriber, event: Event):
        subscriber.on_event(event)


class DefaultSharedPublisher(DefaultPublisher, SharedEventPublisher):
    # todo
    pass
