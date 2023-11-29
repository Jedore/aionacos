from typing import Optional, Dict

from .event import InstanceChangeEvent
from .pojo import ServiceInfo
from .utils import NamingUtils
from .._common import properties
from .._common.center import NOTIFY_CENTER
from .._common.log import logger


class ServiceInfoHolder(object):
    def __init__(self, notifier_event_scop: str):
        self.cache_dir = ""

        is_load_cache = properties.naming_load_cache_at_start

        if is_load_cache:
            # todo read disk file
            ...
        else:
            self._service_info_map = dict()

        self.failover_reactor = FailoverReactor(self, self.cache_dir)

        self.push_empty_protection = properties.naming_push_empty_protection

        self.notifier_event_scope = notifier_event_scop

    @property
    def service_info_map(self) -> Dict[str, ServiceInfo]:
        return self._service_info_map

    def get_service_info(
        self, service_name: str, group_name: str, clusters: str
    ) -> Optional[ServiceInfo]:
        group_service_name = NamingUtils.get_group_name(
            service_name, group_name
        )
        key = ServiceInfo.get_key(group_service_name, clusters)

        # todo failover reactor switch

        return self._service_info_map.get(key)

    def is_empty_or_error_push(self, service_info: ServiceInfo):
        """
        Whether service info is invalid.
        """
        return not service_info.hosts or (
            self.push_empty_protection and not service_info.validate()
        )

    @staticmethod
    def is_changed_service_info(
        old_service: ServiceInfo, new_service: ServiceInfo
    ) -> bool:
        if old_service is None:
            logger.debug(
                f"[Naming] Init new ips {new_service.ip_count()} service: {new_service.get_key2()} -> {new_service.hosts}"
            )
            return True

        if old_service.lastRefTime > new_service.lastRefTime:
            logger.debug(
                f"[Naming] Out of date data received, old-t: {old_service.lastRefTime}, new-t: {new_service.lastRefTime}"
            )
            return False

        changed = False

        old_host_map = {
            host.to_inet_addr(): str(host) for host in old_service.hosts
        }
        new_host_map = {
            host.to_inet_addr(): str(host) for host in new_service.hosts
        }

        mod_hosts = set()
        new_hosts = set()
        remv_hosts = set()

        for key, host in new_host_map.items():
            if key in old_host_map and old_host_map[key] != host:
                mod_hosts.add(host)
            elif key not in old_host_map:
                new_hosts.add(host)

        for key, host in old_host_map.items():
            if key not in new_host_map:
                remv_hosts.add(host)

        if new_hosts:
            changed = True

        if remv_hosts:
            changed = True

        if mod_hosts:
            changed = True

        return changed

    def process_service_info(self, service_info: Optional[ServiceInfo]):
        if service_info is None:
            return

        key = service_info.get_key2()

        old_service = self._service_info_map.get(key)
        if self.is_empty_or_error_push(service_info):
            logger.debug("[Naming] Push empty protection %s.", service_info)
            return old_service

        self._service_info_map[key] = service_info

        if self.is_changed_service_info(old_service, service_info):
            # Publish notify event when service info changed.
            logger.debug(
                "[Naming] Service info changed: %s", service_info.hosts
            )

            NOTIFY_CENTER.publish_event(
                InstanceChangeEvent(
                    self.notifier_event_scope,
                    service_info.name,
                    service_info.groupName,
                    service_info.clusters,
                    service_info.hosts,
                )
            )
        else:
            logger.debug("[Naming] Service info not changed")
            # todo disk write

        return service_info


class FailoverReactor(object):
    def __init__(self, service_info_holder: ServiceInfoHolder, cache_dir: str):
        pass
