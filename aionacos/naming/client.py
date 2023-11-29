import asyncio
import sys
import typing as t

from . import constants as naming_consts
from .pojo import Instance, ServiceInfo
from .push_request_handler import NamingPushRequestHandler
from .request import *
from .response import *
from .service_info_holder import ServiceInfoHolder
from .subscriber import InstanceChangeNotifier
from .utils import NamingUtils
from .._auth.security_proxy import SecurityProxy
from .._common.exceptions import NacosException
from .._common.grpc_client import GrpcClient
from .._common.listener import ConnectionEventListener
from .._common.log import logger
from .._common.redo_service import RedoService
from .._common.response import ResponseType
from .._common.selector import AbstractSelector
from .._common.server_manager import ServerManager


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

        self._grpc = GrpcClient("Naming", server_manager, labels)

        self._security_proxy = SecurityProxy(server_manager.get_server_urls())

        self.redo_service = RedoService("Naming")

        self._update_service = ServiceInfoUpdateService(
            service_info_holder, change_notifier, self
        )

    @property
    def update_service(self):
        return self._update_service

    async def start(self):
        logger.debug("[Naming] Client start ...")

        self._security_proxy.refresh_auth_task()

        self._grpc.register_connection_listener(NamingConnectionListener(self))

        # todo register connection listener
        self._grpc.register_request_handler(
            NamingPushRequestHandler(self._service_info_holder)
        )
        await self._grpc.start()
        # todo register subscriber

    def stop(self):
        logger.debug("[Naming] Client stop ...")
        self._update_service.stop()
        self._security_proxy.stop()
        self._grpc.stop()

    async def _req2server(self, req) -> ResponseType:
        try:
            req.headers.update(self._security_proxy.get_identity_context())
            rsp = await self._grpc.request(req)
            if rsp.success:
                return rsp
            error = f"{req}, {rsp}"
        except Exception as err:
            error = err

        logger.error("[Naming] Request failed: %s", error)
        # todo don't raise
        raise NacosException(NacosException.SERVER_ERR, error)

    async def register(
        self,
        service_name: str,
        group_name: str,
        instance: Instance,
    ):
        # todo redo cache
        req = InstanceRequest(
            instance=instance,
            groupName=group_name,
            serviceName=service_name,
            namespace=self._namespace,
            type_=naming_consts.REGISTER_INSTANCE,
        )
        await self._req2server(req)
        # todo redo instance register
        self.redo_service.add(self.register, service_name, group_name, instance)

    async def deregister(
        self,
        service_name: str,
        group_name: str,
        instance: Instance,
    ):
        # todo redo cache
        req = InstanceRequest(
            instance=instance,
            groupName=group_name,
            serviceName=service_name,
            namespace=self._namespace,
            type_=naming_consts.DE_REGISTER_INSTANCE,
        )
        await self._req2server(req)
        # todo redo instance register

    async def batch_register(
        self,
        service_name: str,
        group_name: str,
        instances: t.List[Instance],
    ):
        req = BatchInstanceRequest(
            instances=instances,
            groupName=group_name,
            serviceName=service_name,
            namespace=self._namespace,
            type_=naming_consts.BATCH_REGISTER_INSTANCE,
        )
        await self._req2server(req)
        # todo redo

    async def query_instance_of_service(
        self,
        service_name: str,
        group_name: str,
        clusters: str,
        udp_port: int,
        healthy_only: bool,
    ) -> ServiceInfo:
        req = ServiceQueryRequest(
            cluster=clusters,
            udpPort=udp_port,
            groupName=group_name,
            serviceName=service_name,
            namespace=self._namespace,
            healthyOnly=healthy_only,
        )
        rsp = await self._req2server(req)  # type: QueryServiceResponse
        return rsp.serviceInfo

    async def subscribe(
        self, service_name: str, group_name: str, clusters: str
    ) -> ServiceInfo:
        group_service_name = NamingUtils.get_group_name(service_name, group_name)
        key = ServiceInfo.get_key(group_service_name, clusters)

        # Schedule update service task.
        self._update_service.schedule_update(service_name, group_name, clusters)

        # Get service info from cache.
        service_info = self._service_info_holder.service_info_map.get(key)
        if service_info is None:
            req = SubscribeServiceRequest(
                subscribe=True,
                clusters=clusters,
                groupName=group_name,
                namespace=self._namespace,
                serviceName=service_name,
            )
            rsp = await self._req2server(req)  # type: SubscribeServiceResponse
            service_info = rsp.serviceInfo

        # Deal service info.
        self._service_info_holder.process_service_info(service_info)

        # Add redo service.
        self.redo_service.add(self.subscribe, service_name, group_name, clusters)

        return service_info

    async def unsubscribe(self, service_name: str, group_name: str, clusters: str):
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
        # todo selector type
        if selector is not None:
            pass
        rsp: ServiceListResponse = await self._req2server(req)
        return rsp.count, rsp.serviceNames

    def server_health(self):
        return self._grpc.is_running()


class NamingConnectionListener(ConnectionEventListener):
    def __init__(self, client: NamingClient):
        self._client = client

    def on_connected(self):
        logger.debug("[Naming] Connection listener, on connected.")
        self._client.redo_service.redo()

    def on_disconnected(self):
        logger.debug("[Naming] Connection listener, on disconnected.")
        self._client.update_service.stop()


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
        """
        Run the update asyncio task.
        """
        logger.debug("[Naming] Start schedule update task: %s", self._name)

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
                        "[Naming] Service update task is stopped: %s",
                        self._service_key,
                    )
                    break

                # Get service info from cache.
                service_info = (
                    self._update_service.service_info_holder.service_info_map.get(
                        self._service_key
                    )
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
                        )
                    )

                    # Process service info.
                    self._update_service.service_info_holder.process_service_info(
                        service_info
                    )

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
                            )
                        )

                        # Process service info.
                        self._update_service.service_info_holder.process_service_info(
                            service_info
                        )

                    # Update task last_ref_time.
                    self._last_ref_time = service_info.lastRefTime

                    # Check service.
                    if service_info.ip_count() == 0:
                        self._inc_fail_count()
                        logger.debug("[Naming] Update task: %s empty", self._name)
                    else:
                        self._reset_fail_count()

                logger.debug(
                    "[Naming] Update task: %s, service: %s",
                    self._name,
                    service_info,
                )

            except asyncio.CancelledError:
                # Stop loop when cancel task.
                break
            except Exception as err:
                self._inc_fail_count()
                logger.error("[Naming] Update task: ,failed: %s", self._name, err)

            # Sleep time before next loop.
            # min 6s no exception, max 60s
            await asyncio.sleep(min(6 << self._fail_count, 60))

        # Make sure ended task not in update service map.
        if self._service_key in self._update_service.service_map:
            self._update_service.service_map.pop(self._service_key)

        logger.debug("[Naming] Stop update task: %s", self._name)


class ServiceInfoUpdateService(object):
    """
    Service for updating service info periodically.
    """

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

    @property
    def service_map(self):
        return self._service_map

    @property
    def change_notifier(self):
        return self._change_notifier

    @property
    def service_info_holder(self):
        return self._service_info_holder

    @property
    def client(self):
        return self._client

    def schedule_update(
        self,
        service_name: str,
        group_name: str,
        clusters: str,
    ):
        """
        Schedule update task if absent.
        """
        service_key = ServiceInfo.get_key(
            NamingUtils.get_group_name(service_name, group_name), clusters
        )
        if service_key in self._service_map:
            return

        self._service_map[service_key] = asyncio.create_task(
            UpdateTask(service_key, service_name, group_name, clusters, self).run()
        )

        logger.debug("[Naming] Schedule update task: %s", service_key)

    async def stop_update(self, service_name: str, group_name: str, clusters: str):
        """
        Stop update task when unsubscribe service.
        """
        service_key = ServiceInfo.get_key(
            NamingUtils.get_group_name(service_name, group_name), clusters
        )

        if service_key in self._service_map:
            task = self._service_map.pop(service_key)
            if not task.done():
                task.cancel()

            logger.debug("[Naming] Stop update task: %s", service_key)

    def stop(self):
        for _, task in self._service_map.items():
            if not task.done():
                task.cancel()

        # Need to clear map when disconnected, redo service will add new task.
        self._service_map.clear()
