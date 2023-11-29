import typing as t
from uuid import uuid4

from .balancer import Balancer
from .client import NamingClient
from .event import InstanceChangeEvent
from .listener import EventListener
from .pojo import Instance, ServiceInfo
from .service_info_holder import ServiceInfoHolder
from .subscriber import InstanceChangeNotifier
from .utils import NamingUtils
from .._common import properties, constants as cst
from .._common.center import NOTIFY_CENTER
from .._common.log import logger
from .._common.selector import AbstractSelector


class NamingService(object):
    def __init__(self):
        self.namespace = properties.naming_namespace or cst.DEFAULT_NAMESPACE_ID

        self._notifier_event_scope = str(uuid4())
        self._change_notifier = InstanceChangeNotifier(self._notifier_event_scope)

        NOTIFY_CENTER.register2publisher(InstanceChangeEvent, 16384)
        NOTIFY_CENTER.register_subscriber(self._change_notifier)

        self._service_info_holder = ServiceInfoHolder(self._notifier_event_scope)

        self._client = NamingClient(
            self.namespace,
            self._service_info_holder,
            self._change_notifier,
        )

    async def start(self):
        logger.info("[Naming] Service start ...")
        await self._client.start()

    def stop(self):
        logger.info("[Naming] Service stop ...")
        self._client.stop()

    async def register_instance(
        self,
        service_name: str,
        ip: str,
        port: int,
        group_name: str = cst.DEFAULT_GROUP,
        cluster_name: str = cst.DEFAULT_CLUSTER_NAME,
        instance: Instance = None,
    ):
        if instance is None:
            instance = Instance(ip=ip, port=port, clusterName=cluster_name)
        NamingUtils.check_instance_legal(instance)
        await self._client.register(service_name, group_name, instance)

    async def deregister_instance(
        self,
        service_name: str,
        ip: str,
        port: int,
        group_name: str = cst.DEFAULT_GROUP,
        cluster_name: str = cst.DEFAULT_CLUSTER_NAME,
        instance: Instance = None,
    ):
        if instance is None:
            instance = Instance(ip=ip, port=port, clusterName=cluster_name)
        await self._client.deregister(service_name, group_name, instance)

    async def batch_register_instance(
        self,
        service_name: str,
        instances: t.List[Instance],
        group_name: str = cst.DEFAULT_GROUP,
    ):
        if not instances:
            return

        await self._client.batch_register(service_name, group_name, instances)

    async def get_all_instances(
        self,
        service_name: str,
        group_name: str = cst.DEFAULT_GROUP,
        clusters: t.List[str] = None,
        subscribe: bool = True,
    ) -> t.List[Instance]:
        clusters = NamingUtils.parse_clusters(clusters)

        if subscribe:
            service_info = self._service_info_holder.get_service_info(
                service_name, group_name, clusters
            )

            if service_info is None:
                service_info = await self._client.subscribe(
                    service_name, group_name, clusters
                )
        else:
            service_info = await self._client.query_instance_of_service(
                service_name, group_name, clusters, 0, False
            )

        return service_info.hosts if service_info else []

    async def select_instances(
        self,
        service_name: str,
        group_name: str = cst.DEFAULT_GROUP,
        clusters: t.List[str] = None,
        healthy: bool = True,
        subscribe: bool = True,
    ) -> t.List[Instance]:
        clusters = NamingUtils.parse_clusters(clusters)

        if subscribe:
            service_info = self._service_info_holder.get_service_info(
                service_name, group_name, clusters
            )
            if service_info is None:
                service_info = await self._client.subscribe(
                    service_name, group_name, clusters
                )
        else:
            service_info = self._client.query_instance_of_service(
                service_name, group_name, clusters, 0, False
            )

        if service_info is None:
            return []
        hosts = []
        for host in service_info.hosts:
            if host.healthy == healthy and host.enabled and host.weight > 0:
                hosts.append(host)
        return hosts

    async def select_one_healthy_instance(
        self,
        service_name: str,
        group_name: str = cst.DEFAULT_GROUP,
        clusters: t.List[str] = None,
        subscribe: bool = True,
    ):
        clusters = NamingUtils.parse_clusters(clusters)

        if subscribe:
            service_info = self._service_info_holder.get_service_info(
                service_name, group_name, clusters
            )
            if service_info is None:
                service_info = await self._client.subscribe(
                    service_name, group_name, clusters
                )
        else:
            service_info = self._client.query_instance_of_service(
                service_name, group_name, clusters, 0, False
            )
        return Balancer.get_host_by_random_weight(service_info)

    async def subscribe(
        self,
        service_name: str,
        listener: EventListener,
        group_name: str = cst.DEFAULT_GROUP,
        clusters: t.List[str] = None,
    ):
        if not listener:
            return

        clusters = NamingUtils.parse_clusters(clusters)
        self._change_notifier.register_listener(
            service_name, group_name, clusters, listener
        )
        return await self._client.subscribe(service_name, group_name, clusters)

    async def unsubscribe(
        self,
        service_name: str,
        listener: EventListener,
        group_name: str = cst.DEFAULT_GROUP,
        clusters: t.List[str] = None,
    ):
        clusters = NamingUtils.parse_clusters(clusters)

        if self._change_notifier.is_subscribed(service_name, group_name, clusters):
            await self._client.unsubscribe(service_name, group_name, clusters)

        self._change_notifier.deregister_listener(
            service_name, group_name, clusters, listener
        )

    async def get_services_of_server(
        self,
        page_no: int,
        page_size: int,
        group_name: str = cst.DEFAULT_GROUP,
        selector: AbstractSelector = None,
    ):
        return await self._client.get_services_list(
            page_no, page_size, group_name, selector
        )

    def get_subscribe_services(self) -> t.List[ServiceInfo]:
        return self._change_notifier.get_subscribe_services()

    def get_server_status(self):
        return "UP" if self._client.server_health() else "DOWN"
