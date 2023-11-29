from typing import List, Dict, Set

from aionacos.naming.event import InstanceChangeEvent
from aionacos.naming.listener import EventListener
from aionacos.naming.pojo import ServiceInfo
from aionacos.naming.utils import NamingUtils
from .._common.subscriber import Subscriber


class InstanceChangeNotifier(Subscriber):
    __slots__ = ("event_scope", "listener_map")

    def __init__(self, event_scope: str):
        self.event_scope = event_scope
        self.listener_map: Dict[str, Set[EventListener]] = {}

    def register_listener(
        self,
        service_name: str,
        group_name: str,
        clusters: str,
        listener: EventListener,
    ):
        key = ServiceInfo.get_key(
            NamingUtils.get_group_name(service_name, group_name), clusters
        )
        event_listeners = self.listener_map.setdefault(key, set())
        event_listeners.add(listener)

    def deregister_listener(
        self,
        service_name: str,
        group_name: str,
        clusters: str,
        listener: EventListener,
    ):
        key = ServiceInfo.get_key(
            NamingUtils.get_group_name(service_name, group_name), clusters
        )
        event_listeners = self.listener_map.get(key)
        if event_listeners is None:
            return

        if listener in event_listeners:
            event_listeners.remove(listener)

        if not event_listeners:
            self.listener_map.pop(key)

    def is_subscribed(
        self, service_name: str, group_name: str, clusters: str
    ) -> bool:
        key = ServiceInfo.get_key(
            NamingUtils.get_group_name(service_name, group_name), clusters
        )
        return bool(self.listener_map.get(key))

    def get_subscribe_services(self) -> List[ServiceInfo]:
        return [ServiceInfo.from_key(key) for key in self.listener_map.keys()]

    def on_event(self, event: InstanceChangeEvent):
        key = ServiceInfo.get_key(
            NamingUtils.get_group_name(event.service_name, event.group_name),
            event.clusters,
        )
        event_listeners = self.listener_map.get(key, set())
        for listener in event_listeners:
            listener.on_event(event)

    @staticmethod
    def subscribe_type():
        return InstanceChangeEvent

    def scope_matches(self, event: InstanceChangeEvent):
        return self.event_scope == event.scope
