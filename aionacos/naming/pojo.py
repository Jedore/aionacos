from typing import List
from urllib.parse import quote_plus

from . import preserved_metatdata_keys as pmk
from .._common import constants as cst
from .._common.payload import SlotsMeta
from .._common.utils import Serializable


class Instance(Serializable, metaclass=SlotsMeta):
    def __init__(
        self,
        instanceId: str = "",
        clusterName: str = "",
        serviceName: str = "",
        serialVersionUI: int = -742906310567291979,
        ip: str = "",
        port: int = 0,
        weight: float = 1.0,
        healthy: bool = True,
        enabled: bool = True,
        ephemeral: bool = True,
        metadata: dict = None,
        **kwargs,
    ):
        self.instanceId = instanceId
        self.clusterName = clusterName
        self.serviceName = serviceName
        self.serialVersionUID = serialVersionUI
        self.ip = ip
        self.port = port
        self.weight = weight
        self.healthy = healthy
        self.enabled = enabled
        self.ephemeral = ephemeral
        self.metadata = {} if metadata is None else metadata

    @property
    def heartbeat_timeout(self):
        return self.metadata.get(pmk.HEART_BEAT_TIMEOUT, cst.DEFAULT_HEART_BEAT_TIMEOUT)

    @property
    def heartbeat_interval(self):
        return self.metadata.get(
            pmk.HEART_BEAT_INTERVAL, cst.DEFAULT_HEART_BEAT_INTERVAL
        )

    @property
    def ip_del_timeout(self):
        return self.metadata.get(pmk.IP_DELETE_TIMEOUT, cst.DEFAULT_IP_DELETE_TIMEOUT)

    def to_inet_addr(self):
        return f"{self.ip}:{self.port}"


class Cluster(object):
    pass


class ListView(object):
    pass


class Service(object):
    pass


class ServiceInfo(Serializable, metaclass=SlotsMeta):
    def __init__(
        self,
        name: str = "",
        checksum: str = "",
        clusters: str = "",
        groupName: str = "",
        valid: bool = False,
        allIPs: bool = False,
        lastRefTime: int = 0,
        hosts: List[dict] = None,
        cacheMillis: int = 1000,
        reachProtectionThreshold: bool = False,
        **kwargs,
    ):
        self.hosts = []
        if hosts is not None:
            for host in hosts:
                self.hosts.append(Instance(**host))

        self.clusters = clusters
        self.name = name
        self.groupName = groupName
        self.checksum = checksum
        self.valid = valid
        self.allIPs = allIPs
        self.lastRefTime = lastRefTime
        self.cacheMillis = cacheMillis
        self.reachProtectionThreshold = reachProtectionThreshold

    def get_grouped_service_name(self):
        if self.groupName and cst.SERVICE_INFO_SPLITER not in self.name:
            return self.groupName + cst.SERVICE_INFO_SPLITER + self.name
        return self.name

    @staticmethod
    def get_key(group_service_name: str, clusters: str):
        # todo 参数改为关键字参数 get key 2 合一
        if not clusters:
            return group_service_name
        return group_service_name + cst.SERVICE_INFO_SPLITER + clusters

    def get_key2(self):
        return self.get_key(self.get_grouped_service_name(), self.clusters)

    def get_key_encoded(self):
        return self.get_key(
            quote_plus(self.get_grouped_service_name(), encoding="utf8"),
            self.clusters,
        )

    @staticmethod
    def from_key(key: str):
        service_info = ServiceInfo()
        segments = key.split(cst.SERVICE_INFO_SPLITER)
        service_info.groupName = segments[0]
        service_info.name = segments[1]
        if len(segments) == 3:
            service_info.clusters = segments[2]
        return service_info

    def validate(self) -> bool:
        """
        Check whether service info is valid.
        """

        if self.allIPs:
            return True

        for host in self.hosts:
            if host.healthy and host.weight > 0:
                return True

        return False

    def ip_count(self):
        """
        hosts count
        """
        return len(self.hosts)
