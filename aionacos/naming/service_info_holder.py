import typing as t
from pathlib import Path

from . import disk_cache
from .event import InstanceChangeEvent
from .pojo import ServiceInfo
from .utils import NamingUtils
from ..common import conf
from ..common.center import NOTIFY_CENTER
from ..common.log import logger


class ServiceInfoHolder(object):
    def __init__(self, notifier_event_scop: str, cache_dir: Path):
        self._cache_dir = cache_dir

        is_load_cache = conf.naming_load_cache_at_start

        if is_load_cache:
            self._service_info_map = disk_cache.read(self._cache_dir)
        else:
            self._service_info_map = {}

        self.failover_reactor = FailoverReactor(self, self._cache_dir)

        self.push_empty_protection = conf.naming_push_empty_protection

        self.notifier_event_scope = notifier_event_scop

    @property
    def service_map(self) -> t.Dict[str, ServiceInfo]:
        return self._service_info_map

    def get_service_info(
        self, service_name: str, group_name: str, clusters: str
    ) -> t.Optional[ServiceInfo]:
        group_service_name = NamingUtils.get_group_name(service_name, group_name)
        key = ServiceInfo.get_key(group_service_name, clusters)

        # todo failover reactor switch

        return self._service_info_map.get(key)

    def is_empty_or_error_push(self, service_info: ServiceInfo) -> bool:
        if not service_info.hosts:
            return True
        if self.push_empty_protection and not service_info.validate():
            return True

    @staticmethod
    def is_changed_service_info(
        old_service: ServiceInfo, new_service: ServiceInfo
    ) -> bool:
        if old_service is None:
            logger.debug(
                f"[Naming] Init new ips {new_service.ip_count()} "
                f"service: {new_service.get_key2()} -> {new_service.hosts}"
            )
            return True

        if old_service.lastRefTime > new_service.lastRefTime:
            logger.debug(
                f"[Naming] Out of date data received, "
                f"old-t: {old_service.lastRefTime}, new-t: {new_service.lastRefTime}"
            )
            return False

        old_host_map = {host.to_inet_addr(): str(host) for host in old_service.hosts}
        new_host_map = {host.to_inet_addr(): str(host) for host in new_service.hosts}

        for key, host in new_host_map.items():
            if old_host_map.get(key) != host:
                return True

        for key, host in old_host_map.items():
            if key not in new_host_map:
                return True

        return False

    def process_service(self, service_info: t.Optional[ServiceInfo]):
        if service_info is None:
            return

        key = service_info.get_key2()

        old_service_info = self._service_info_map.get(key)

        if self.is_empty_or_error_push(service_info):
            logger.debug("[Naming] push empty protection %s.", service_info)
            # why return old service
            # return old_service_info

        self._service_info_map[key] = service_info

        if self.is_changed_service_info(old_service_info, service_info):
            # Publish notify event when service info changed.
            logger.debug("[Naming] service info changed: %s", service_info.hosts)
            NOTIFY_CENTER.publish_event(
                InstanceChangeEvent(
                    self.notifier_event_scope,
                    service_info.name,
                    service_info.groupName,
                    service_info.clusters,
                    service_info.hosts,
                )
            )
            disk_cache.write(service_info, self._cache_dir)
        else:
            logger.debug("[Naming] service info not changed")

        # why return service
        # return service_info


class FailoverReactor(object):
    def __init__(self, service_info_holder: ServiceInfoHolder, cache_dir: Path):
        pass
