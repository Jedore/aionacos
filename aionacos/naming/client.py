import asyncio
import sys
import typing as t

from . import constants as naming_consts
from .pojo import Instance, ServiceInfo
from .push_req_handler import NamingPushRequestHandler
from .request import *
from .response import *
from .service_info_holder import ServiceInfoHolder
from .subscriber import InstanceChangeNotifier
from .utils import NamingUtils
from .._auth.security_proxy import SecurityProxy
from ..common import GrpcClient
from ..common.exceptions import NacosException
from ..common.listener import ConnectionEventListener
from ..common.log import logger
from ..common.response import ResponseType
from ..common.selector import AbstractSelector
from ..common.server_manager import ServerManager

NAMING = "Naming"


class NamingClient(object):
    def __init__(
        self,
        namespace: str,
        service_info_holder: ServiceInfoHolder,
        change_notifier: InstanceChangeNotifier,
    ):
        labels = {"module": "naming", "source": "sdk"}
        self._namespace = namespace
        self._service_info_holder = service_info_holder
        server_manager = ServerManager()
        self._grpc = GrpcClient(NAMING, server_manager, labels)
        self._security_proxy = SecurityProxy(NAMING, server_manager.get_server_urls())
        self._redo = NamingRedoService(self)
        self._update = ServiceInfoUpdateService(
            service_info_holder, change_notifier, self
        )

    @property
    def update(self):
        return self._update

    async def start(self):
        logger.debug("[Naming] client start")
        self._security_proxy.refresh_auth_task()
        self._grpc.register_connection_listener(self._redo)
        self._grpc.register_request_handler(
            NamingPushRequestHandler(self._service_info_holder)
        )
        await self._grpc.start()

    def stop(self):
        logger.debug("[Naming] client stop")
        self._update.stop()
        self._security_proxy.stop()
        self._grpc.stop()

    async def _req2server(self, req, throw: bool = True) -> ResponseType:
        req.headers.update(self._security_proxy.get_identity_context())
        rsp: ResponseType = await self._grpc.request(req, throw=throw)
        if rsp.success:
            return rsp

        # when request failed
        logger.error("[Naming] request failed: %s, %s", req, rsp)
        if throw:
            NacosException(rsp.errorCode, rsp.message)

    async def register(
        self, service_name: str, group_name: str, instance: Instance, throw: bool = True
    ):
        req = InstanceRequest(
            instance=instance,
            groupName=group_name,
            serviceName=service_name,
            namespace=self._namespace,
            type_=naming_consts.REGISTER_INSTANCE,
        )
        await self._req2server(req, throw=throw)

        # cache after success
        self._redo.cache_register(service_name, group_name, instance)

    async def deregister(self, service_name: str, group_name: str, instance: Instance):
        # remove cache after success
        self._redo.cache_deregister(service_name, group_name)

        req = InstanceRequest(
            instance=instance,
            groupName=group_name,
            serviceName=service_name,
            namespace=self._namespace,
            type_=naming_consts.DE_REGISTER_INSTANCE,
        )
        await self._req2server(req)

    async def batch_register(
        self, service_name: str, group_name: str, instances: t.List[Instance]
    ):
        # todo support batch register, for what scenario
        pass

    async def query_instance_of_service(
        self,
        service_name: str,
        group_name: str,
        clusters: str,
        udp_port: int,
        healthy_only: bool,
        throw: bool = True,
    ) -> ServiceInfo:
        req = ServiceQueryRequest(
            cluster=clusters,
            udpPort=udp_port,
            groupName=group_name,
            serviceName=service_name,
            namespace=self._namespace,
            healthyOnly=healthy_only,
        )
        rsp: QueryServiceResponse = await self._req2server(req, throw=throw)
        return rsp.serviceInfo

    async def subscribe(
        self, service_name: str, group_name: str, clusters: str, throw: bool = True
    ) -> ServiceInfo:
        group_service_name = NamingUtils.get_group_name(service_name, group_name)
        key = ServiceInfo.get_key(group_service_name, clusters)
        # Get service info from cache.
        service_info = self._service_info_holder.service_map.get(key)
        if service_info is None:
            req = SubscribeServiceRequest(
                subscribe=True,
                clusters=clusters,
                groupName=group_name,
                namespace=self._namespace,
                serviceName=service_name,
            )
            rsp: SubscribeServiceResponse = await self._req2server(req, throw=throw)
            service_info = rsp.serviceInfo

            # only process new service info
            self._service_info_holder.process_service(service_info)

        # schedule update task if service info valid
        self._update.schedule_update(service_name, group_name, clusters)
        # cache after if service info valid
        self._redo.cache_subscribe(service_name, group_name, clusters)
        return service_info

    async def unsubscribe(self, service_name: str, group_name: str, clusters: str):
        # stop update service task
        self._update.stop_update(service_name, group_name, clusters)
        # remove cache
        self._redo.cache_unsubscribe(service_name, group_name, clusters)

        req = SubscribeServiceRequest(
            subscribe=False,
            clusters=clusters,
            groupName=group_name,
            namespace=self._namespace,
            serviceName=service_name,
        )
        await self._req2server(req)

    async def get_services_list(
        self,
        page_no: int,
        page_size: int,
        group_name: str,
        selector: AbstractSelector = None,
    ):
        req = ServiceListRequest(
            pageNo=page_no,
            pageSize=page_size,
            groupName=group_name,
            namespace=self._namespace,
        )
        # todo support selector
        # if selector is not None:
        #     pass
        rsp: ServiceListResponse = await self._req2server(req)
        return rsp.count, rsp.serviceNames

    def server_health(self):
        return self._grpc.is_running()


class UpdateTask(object):
    def __init__(
        self,
        service_key: str,
        service_name: str,
        group_name: str,
        clusters: str,
        update_service: "ServiceInfoUpdateService",
    ):
        self._service_key = self._name = service_key
        self._service_name = service_name
        self._group_name = group_name
        self._clusters = clusters
        self._group_service_name = NamingUtils.get_group_name(service_name, group_name)
        self._update_service = update_service
        self._last_ref_time = sys.maxsize
        self._fail_count = 0
        self._fail_limit = 6

    def _inc_fail_count(self):
        """
        Increase fail count by 1.
        """
        if self._fail_count == self._fail_limit:
            return
        self._fail_count += 1

    def _reset_fail_count(self):
        """
        Reset fail count to 0.
        """
        self._fail_count = 0

    async def run(self):
        logger.debug("[Naming] schedule update task: %s", self._name)

        await asyncio.sleep(3)

        while True:
            try:
                # If unsubscribed or not in service map, break and end task.
                if (
                    self._service_key not in self._update_service.service_map
                    or not self._update_service.change_notifier.is_subscribed(
                        self._service_name, self._group_name, self._clusters
                    )
                ):
                    logger.debug(
                        "[Naming] service update task is stopped: %s",
                        self._service_key,
                    )
                    break

                # Get service info from cache.
                service_info = self._update_service.service_holder.service_map.get(
                    self._service_key
                )

                # Service info not int cache.
                if service_info is None:
                    # Query from server
                    service_info = (
                        await self._update_service.client.query_instance_of_service(
                            self._service_name,
                            self._group_name,
                            self._clusters,
                            0,
                            False,
                            throw=False,
                        )
                    )

                    # Process service info.
                    self._update_service.service_holder.process_service(service_info)

                    # Update task last_ref_time.
                    self._last_ref_time = service_info.lastRefTime

                # Cache service info is expired.
                else:
                    if service_info.lastRefTime <= self._last_ref_time:
                        # Query from server.
                        service_info = (
                            await self._update_service.client.query_instance_of_service(
                                self._service_name,
                                self._group_name,
                                self._clusters,
                                0,
                                False,
                                throw=False,
                            )
                        )

                        # Process service info.
                        self._update_service.service_holder.process_service(
                            service_info
                        )

                    # Update task last_ref_time.
                    self._last_ref_time = service_info.lastRefTime

                    # Check service.
                    if service_info.ip_count() == 0:
                        self._inc_fail_count()
                        logger.debug("[Naming] update task: %s empty", self._name)
                    else:
                        self._reset_fail_count()

                logger.debug("[Naming] update task: %s, %s", self._name, service_info)

            except asyncio.CancelledError:
                # Stop loop when cancel task.
                break
            except Exception as err:
                self._inc_fail_count()
                logger.error("[Naming] update task failed: %s, %s", self._name, err)

            # Sleep time before next loop.
            # min 6s no exception, max 60s
            await asyncio.sleep(min(6 << self._fail_count, 60))

        # Make sure ended task not in update service map.
        if self._service_key in self._update_service.service_map:
            self._update_service.service_map.pop(self._service_key)

        logger.debug("[Naming] stop update task: %s", self._name)


class ServiceInfoUpdateService(object):
    def __init__(
        self,
        service_info_holder: ServiceInfoHolder,
        change_notifier: InstanceChangeNotifier,
        naming_client: NamingClient,
    ):
        self._service_info_holder = service_info_holder
        self._change_notifier = change_notifier
        self._service_map: t.Dict[str, asyncio.Task] = {}
        self._client = naming_client
        # todo support configuration for update service or not
        self._is_async_query_for_subscribed_service: bool = True

    @property
    def service_map(self):
        return self._service_map

    @property
    def change_notifier(self):
        return self._change_notifier

    @property
    def service_holder(self):
        return self._service_info_holder

    @property
    def client(self):
        return self._client

    def schedule_update(self, service_name: str, group_name: str, clusters: str):
        if not self._is_async_query_for_subscribed_service:
            return

        service_key = ServiceInfo.get_key(
            NamingUtils.get_group_name(service_name, group_name), clusters
        )
        if service_key in self._service_map:
            return

        # todo multi subscribed services share single task
        self._service_map[service_key] = asyncio.create_task(
            UpdateTask(service_key, service_name, group_name, clusters, self).run()
        )
        # logger.debug("[Naming] schedule update task: %s", service_key)

    def stop_update(self, service_name: str, group_name: str, clusters: str):
        service_key = ServiceInfo.get_key(
            NamingUtils.get_group_name(service_name, group_name), clusters
        )

        # todo multi subscribed services share single task
        if service_key in self._service_map:
            task = self._service_map.pop(service_key)
            if not task.done():
                task.cancel()

            logger.debug("[Naming] stop update task: %s", service_key)

    def stop(self):
        for _, task in self._service_map.items():
            if not task.done():
                task.cancel()

        # Need to clear map when disconnected, redo service will add new task.
        self._service_map.clear()


class NamingRedoService(ConnectionEventListener):
    def __init__(self, client: NamingClient):
        self._client = client
        self._is_first_connected = True
        self._register_map = {}
        self._subscribe_map = {}

    def on_connected(self):
        asyncio.create_task(self.run())
        # logger.debug("[Naming] on connected.")

    def on_disconnected(self):
        self._client.update.stop()
        # logger.debug("[Naming] on disconnected.")

    def cache_register(self, service_name: str, group_name: str, instance: Instance):
        # assume one application can only register one instance with same group&service
        key = NamingUtils.get_group_name(service_name, group_name)
        self._register_map[key] = (service_name, group_name, instance)

    def cache_deregister(self, service_name: str, group_name: str):
        key = NamingUtils.get_group_name(service_name, group_name)
        self._register_map.pop(key, None)

    def cache_subscribe(self, service_name: str, group_name: str, clusters: str):
        key = ServiceInfo.get_key(
            NamingUtils.get_group_name(service_name, group_name), clusters
        )
        self._subscribe_map[key] = (service_name, group_name, clusters)

    def cache_unsubscribe(self, service_name: str, group_name: str, clusters: str):
        key = ServiceInfo.get_key(
            NamingUtils.get_group_name(service_name, group_name), clusters
        )
        self._subscribe_map.pop(key, None)

    async def run(self):
        # Do not redo when connect firstly.
        if self._is_first_connected:
            self._is_first_connected = False
            return

        logger.debug("[Naming] redo service start")
        for _, args in self._register_map.items():
            await self._client.register(*args, throw=False)
        for _, args in self._subscribe_map.items():
            await self._client.subscribe(*args, throw=False)
