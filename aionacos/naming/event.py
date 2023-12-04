from typing import List

from .pojo import Instance
from ..common.event import Event, SlowEvent


class InstanceChangeEvent(Event):
    __slots__ = Event.__slots__ + (
        "event_scope",
        "service_name",
        "group_name",
        "clusters",
        "hosts",
    )

    def __init__(
        self,
        eventScope: str,
        serviceName: str,
        groupName: str,
        clusters: str,
        hosts: List[Instance],
    ):
        super().__init__()

        self.serialVersionUI = -8823087028212249603
        self.event_scope = eventScope
        self.service_name = serviceName
        self.group_name = groupName
        self.clusters = clusters
        self.hosts = hosts

    @property
    def scope(self):
        return self.event_scope


class ServerListChangeEvent(SlowEvent):
    pass
