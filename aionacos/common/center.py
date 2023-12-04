from typing import Dict

from .event import Event, SlowEvent
from .log import logger
from .publisher import EventPublisher, SharedEventPublisher, DefaultPublisher
from .subscriber import Subscriber, SmartSubscriber


class NotifyCenter(object):
    def __init__(self):
        self.publisher_map: Dict[str, EventPublisher] = {}

    def register2publisher(
        self,
        event_type: type,
        queue_max_size: int,
        event_publisher_class: type = DefaultPublisher,
    ):
        # todo shared publisher
        if issubclass(event_type, SlowEvent):
            return

        topic = event_type.__name__

        publisher = event_publisher_class()
        publisher.init(event_type, queue_max_size)
        self.publisher_map[topic] = publisher
        return publisher

    def register_subscriber(self, consumer: Subscriber):
        # todo smart subscribers
        subscribe_type = consumer.subscribe_type()
        if isinstance(consumer, SmartSubscriber):
            pass
        # todo slow event

        self.add_subscriber(consumer, subscribe_type)

    def deregister_subscriber(self, consumer: Subscriber):
        if isinstance(consumer, SmartSubscriber):
            pass

        subscribe_type = consumer.subscribe_type()
        if issubclass(subscribe_type, SlowEvent):
            # todo
            return

        self.remove_subscriber(consumer, subscribe_type)

    @staticmethod
    def add_subscriber(consumer: Subscriber, subscribe_type: type):
        topic = subscribe_type.__name__
        publisher = NOTIFY_CENTER.publisher_map.get(topic)
        if isinstance(publisher, SharedEventPublisher):
            # todo shared publisher
            pass
        else:
            publisher.add_subscriber(consumer)

    @staticmethod
    def remove_subscriber(consumer: Subscriber, subscribe_type: type):
        topic = subscribe_type.__name__
        event_publisher = NOTIFY_CENTER.publisher_map.get(topic)
        if event_publisher is not None:
            if isinstance(consumer, SharedEventPublisher):
                # todo
                pass
            else:
                event_publisher.remove_subscriber(consumer)
        return True

    def publish_event(self, event: Event):
        # todo
        if issubclass(event.__class__, SlowEvent):
            pass

        topic = event.__class__.__name__
        publisher = self.publisher_map.get(topic)
        if publisher is not None:
            return publisher.publish(event)

        logger.debug("There are no publishers for this event: %s", topic)


# todo singleton
NOTIFY_CENTER = NotifyCenter()
