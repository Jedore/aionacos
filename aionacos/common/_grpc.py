import asyncio
import copy
import enum
import typing as t
import logging

import grpc
import orjson

from . import constants as cst
from . import utils
from .exceptions import NacosException, RemoteException
from .listener import ConnectionEventListener
from .log import logger
from .payload import PAYLOAD_REGISTRY
from .request import *
from .response import *
from .server_manager import ServerManager
from .server_req_handler import ServerRequestHandler
from ..protocol import *


class GrpcStatus(str, enum.Enum):
    def __str__(self):
        return self.value

    INITIALIZING = "grpc is initializing"
    STARTING = "grpc is starting"
    RUNNING = "grpc is running"
    UNHEALTHY = "grpc is unhealthy"
    SHUTDOWN = "grpc is shutdown"


class ClientDetectionRequestHandler(ServerRequestHandler):
    async def request_reply(self, req: Request):
        if isinstance(req, ClientDetectionRequest):
            return ClientDetectionResponse()


class ConnectResetRequestHandler(ServerRequestHandler):
    def __init__(self, client: "GrpcClient"):
        self._client = client

    async def request_reply(self, req: Request):
        if isinstance(req, ConnectResetRequest):
            if self._client.is_running():
                if req.serverIp:
                    server_addr = f"{req.serverIp}{cst.COLON}{req.serverPort}"
                    await self._client.switch_server_async(addr=server_addr)
                else:
                    await self._client.switch_server_async()
            return ConnectResetResponse()


class ReconnectCxt(object):
    def __init__(self, server_addr: str = None, on_request_fail: bool = True):
        self.server_addr = server_addr
        self.on_req_fail = on_request_fail


class ConnectionEvent(object):
    CONNECTED = 1
    DISCONNECTED = 0

    def __init__(self, event_type: int):
        self._event_type = event_type

    def is_connected(self):
        return self._event_type == self.CONNECTED

    def is_disconnected(self):
        return self._event_type == self.DISCONNECTED


class Connection(object):
    def __init__(self, name: str, server_addr: str, param: int = 0):
        self._name = name
        self._server_addr = server_addr
        self._channel = grpc.aio.insecure_channel(
            self._server_addr,
            options=(("grpc.experimental.tcp_min_read_chunk_size", param),),
        )
        self._stub = RequestStub(self._channel)
        self._bi_stub = BiRequestStreamStub(self._channel)
        self._bi_stream = self._bi_stub.requestBiStream()
        self.conn_id: t.Optional[str] = None
        # self.abandon = False

    @property
    def server_addr(self):
        return self._server_addr

    @staticmethod
    def _req2payload(req: Request) -> Payload:
        _req = copy.deepcopy(req)
        pd = Payload()
        pd.metadata.clientIp = utils.local_ip()
        pd.metadata.type = _req.cls_name
        pd.metadata.headers.update(_req.headers)
        _req.headers.clear()
        pd.body.value = orjson.dumps(_req, default=utils.default)
        return pd

    @staticmethod
    def _rsp2payload(rsp: Response) -> Payload:
        pd = Payload()
        pd.metadata.type = rsp.cls_name
        pd.body.value = orjson.dumps(rsp, default=utils.default)
        return pd

    @staticmethod
    def _parse(pd: Payload) -> t.Optional[t.Union[ResponseType, RequestType]]:
        if not hasattr(pd, "body"):
            return

        data: dict = orjson.loads(pd.body.value)
        # "type" is reserved
        if "type" in data:
            data["type_"] = data.pop("type")

        payload_type = PAYLOAD_REGISTRY.get(pd.metadata.type)
        if payload_type is None:
            raise RemoteException(
                NacosException.SERVER_ERR,
                msg="Unknown payload type:" + pd.metadata.type,
            )
        return payload_type(**data)

    async def request(
        self, req: Request, timeout: float = cst.READ_TIMEOUT
    ) -> ResponseType:
        rsp = await self._stub.request(self._req2payload(req), timeout=timeout)
        return self._parse(rsp)

    async def bi_request(self, req: Request):
        await self._bi_stream.write(self._req2payload(req))

    async def bi_response(self, rsp: Response):
        await self._bi_stream.write(self._rsp2payload(rsp))

    async def read_server_request(self) -> t.Optional[RequestType]:
        pd = await self._bi_stream.read()
        if pd is grpc.aio.EOF:
            # The stream is end, new a stream.
            self._bi_stream = self._bi_stub.requestBiStream()
            return
        return self._parse(pd)

    async def close(self):
        if self._channel:
            if self._channel.get_state() == grpc.ChannelConnectivity.SHUTDOWN:
                return

            try:
                await self._channel.close()
            except Exception as err:
                logger.error("[%s] close connection failed: %s", self._name, err)
            finally:
                self._channel = None


class GrpcClient(object):
    KEEP_ALIVE_TIME = 6  # second
    RETRY_TIMES = 3

    def __init__(self, service: str, server_manager: ServerManager, labels: dict):
        self._service = service
        self._server_manager = server_manager
        # todo check
        self._labels = labels
        self._conn: t.Optional[Connection] = None
        self._last_active_time: int = utils.timestamp()  # second
        self._server_req_handlers: t.List[ServerRequestHandler] = []
        self._connection_listeners: t.List[ConnectionEventListener] = []
        self._switch_queue: asyncio.Queue[ReconnectCxt] = asyncio.Queue(maxsize=1)
        self._conn_event_queue: asyncio.Queue[ConnectionEvent] = asyncio.Queue()

        self._status = GrpcStatus.INITIALIZING

        self._tasks: t.List[asyncio.Task] = []
        self._server_request_task: t.Optional[asyncio.Task] = None

    @property
    def service(self):
        return self._service

    def is_initializing(self) -> bool:
        return self._status == GrpcStatus.INITIALIZING

    def is_starting(self) -> bool:
        return self._status == GrpcStatus.STARTING

    def is_running(self) -> bool:
        return self._status == GrpcStatus.RUNNING

    def is_unhealthy(self) -> bool:
        return self._status == GrpcStatus.UNHEALTHY

    def is_shutdown(self) -> bool:
        return self._status == GrpcStatus.SHUTDOWN

    def set_starting(self):
        self._status = GrpcStatus.STARTING
        # logger.debug("[%s] %s", self._name, self._status.value)

    def set_running(self):
        self._status = GrpcStatus.RUNNING
        # logger.info("[%s] %s", self._name, self._status)

    def set_unhealthy(self):
        self._status = GrpcStatus.UNHEALTHY
        # logger.debug("[%s] %s", self._name, self._status)

    def set_shutdown(self):
        self._status = GrpcStatus.SHUTDOWN
        # logger.info("[%s] %s", self._name, self._status)

    async def start(self):
        self.set_starting()

        # register request handlers.
        self.register_request_handler(ClientDetectionRequestHandler())
        self.register_request_handler(ConnectResetRequestHandler(self))

        # Try to connect to the same server multi times.
        retry_times = self.RETRY_TIMES
        while retry_times:
            addr = None
            try:
                addr = self._server_manager.next_server()
                self._conn = await self._connect2server(addr)

                if self._conn is not None:
                    logger.debug(
                        "[%s] connection: %s", self._service, self._conn.conn_id
                    )
                    self._add_conn_event(ConnectionEvent.CONNECTED)
                    self.set_running()

                    # Jump out loop.
                    break
            except Exception as err:
                logger.debug("[%s] connect failed, %s: %s", self._service, addr, err)

            # Minus 1 when connect failed.
            retry_times -= 1

        if self._conn is None:
            await self.switch_server_async()

        # Create background tasks.
        self._tasks.append(asyncio.create_task(self._switch_server()))
        self._tasks.append(asyncio.create_task(self._conn_event_notify()))

    def stop(self):
        logger.info("[%s] grpc shutdown.", self._service)

        self.set_shutdown()

        for task in self._tasks:
            if not task.done():
                task.cancel()
        self._tasks.clear()

        self._conn.close()

    def _add_conn_event(self, event_type: int):
        self._conn_event_queue.put_nowait(ConnectionEvent(event_type))

    async def _server_check(self, conn: Connection) -> t.Optional[str]:
        # logger.debug("[%s] server check: %s", self._name, conn.addr)

        try:
            rsp = await conn.request(ServerCheckRequest(), cst.READ_TIMEOUT)
            if not isinstance(rsp, ServerCheckResponse):
                await conn.close()
            elif not rsp.success:
                logger.debug("[%s] server check failed: %s", self._service, rsp.message)
            else:
                return rsp.connectionId

        except grpc.aio.AioRpcError as err:
            logger.error("[%s] server check failed: %s", self._service, err.details())
        except Exception as err:
            logger.debug("[%s] server check failed: %s", self._service, err)

    async def _connect2server(self, server_addr: str) -> t.Optional[Connection]:
        # logger.debug("[%s] server: %s", self._name, server_addr)

        # 8192/8193 is for creating 2 different connection for naming and config.
        # todo find better way
        if self._service == "Naming":
            conn = Connection(self._service, server_addr, param=8192)
        else:
            conn = Connection(self._service, server_addr, param=8193)

        # Server check.
        conn.conn_id = await self._server_check(conn)

        # Check failed.
        if conn.conn_id is None:
            return

        # Set up connection.
        await conn.bi_request(
            ConnectionSetupRequest(
                labels=self._labels,
                clientVersion=cst.CLIENT_VERSION,
            )
        )

        # Wait to set up connection done.
        await asyncio.sleep(1)

        return conn

    async def _close_connection(self):
        if self._conn:
            logger.debug("[%s] close connection: %s", self._service, self._conn.conn_id)
            self.set_unhealthy()
            await self._conn.close()
            self._conn = None
            self._add_conn_event(ConnectionEvent.DISCONNECTED)

    def _set_active_time(self):
        self._last_active_time = utils.timestamp()

    def register_request_handler(self, handler: ServerRequestHandler):
        self._server_req_handlers.append(handler)
        # logger.debug("[%s] req handler: %s", self._name, handler.name)

    def register_connection_listener(self, listener: ConnectionEventListener):
        self._connection_listeners.append(listener)
        # logger.debug("[%s] connection listener: %s", self._name, listener.name)

    async def request(
        self, req: Request, timeout: float = cst.READ_TIMEOUT, throw: bool = True
    ) -> t.Optional[ResponseType]:
        retry_times = 0
        t_start = utils.timestamp()
        error = None
        while retry_times < self.RETRY_TIMES and utils.timestamp() < t_start + timeout:
            wait_reconnect = False
            try:
                # abnormal connection
                if self._conn is None or not self.is_running():
                    wait_reconnect = True
                    raise NacosException(
                        NacosException.CLIENT_DISCONNECT, msg=self._status
                    )

                # logger.debug("[%s] request server: %s", self._service, req)
                rsp = await self._conn.request(req, timeout=timeout)
                # logger.debug("[%s] server respond: %s", self._name, rsp)

                # error response
                if isinstance(rsp, ErrorResponse):
                    if rsp.errorCode == NacosException.UN_REGISTER:
                        wait_reconnect = True
                        self.set_unhealthy()
                        await self.switch_server_async()

                    # raise error, then retry.
                    raise NacosException(rsp.errorCode, msg=rsp.message)

                # log error
                if rsp is None:
                    logger.error("[%s] server respond None", self._service)
                elif not rsp.success:
                    logger.error(
                        "[%s] server respond error: %s %s",
                        self._service,
                        rsp.errorCode,
                        rsp.message,
                    )

                # update active time
                self._set_active_time()
                return rsp

            except Exception as err:
                # wait before next retry loop when reconnect
                if wait_reconnect:
                    await asyncio.sleep(min(0.1, timeout / 3))
                logger.debug(
                    "[%s] request failed, retry_times: %s, req: %s, error: %s",
                    self._service,
                    retry_times,
                    req,
                    err,
                )
                error = str(err)

            retry_times += 1

        # Switch server asynchronously when always failed.
        if self.is_running():
            self.set_unhealthy()
            await self.switch_server_async(on_req_fail=True)

        # raise when not success
        if throw:
            raise NacosException(NacosException.SERVER_ERR, msg=error)

    async def _health_check(self) -> bool:
        if self._conn is None:
            return False

        if utils.timestamp() - self._last_active_time <= self.KEEP_ALIVE_TIME:
            return True

        # logger.debug("[%s] health check", self._name)

        try:
            rsp = await self.request(HealthCheckRequest(), throw=False)
            if rsp and rsp.success:
                return True
            else:
                logger.debug("[%s] health check failed: %s", self._service, rsp)
        except Exception as err:
            logger.debug("[%s] health check failed: %s", self._service, err)

        return False

    async def switch_server_async(self, addr: str = None, on_req_fail: bool = False):
        try:
            self._switch_queue.put_nowait(
                ReconnectCxt(server_addr=addr, on_request_fail=on_req_fail)
            )
        except asyncio.QueueFull:
            # skip
            pass
        except Exception as err:
            logger.error("[%s] put _switch_queue failed: %s", self._service, err)

    async def _handle_server_request(self):
        # logger.debug("[%s] wait server req.", self._name)

        while self.is_running():
            try:
                req = await self._conn.read_server_request()
                # logger.debug("[%s] server request: %s.", self._name, req)

                if req is None:
                    continue

                for handler in self._server_req_handlers:
                    rsp = await handler.request_reply(req)
                    if rsp is not None:
                        rsp.requestId = req.requestId
                        await self._conn.bi_response(rsp)
                        # logger.debug("[%s] respond server: %s", self._name, rsp)
                        break
            except grpc.aio.AioRpcError as err:
                error = f"{err.code()} {err.details()}"
                logger.debug(
                    "[%s] handle server request failed: %s", self._service, error
                )
                # todo why
                await asyncio.sleep(self.KEEP_ALIVE_TIME)

            except asyncio.CancelledError:
                # Stop loop when cancel task.
                break
            except Exception as err:
                logger.debug(
                    "[%s] handle server request failed: %s", self._service, err
                )

    async def _switch_server(self):
        # logger.debug("[%s] get _switch_server queue & health check.", self._name)

        while not self.is_shutdown():
            try:
                try:
                    # Get reconnect context
                    cxt: t.Optional[ReconnectCxt] = await asyncio.wait_for(
                        self._switch_queue.get(), self.KEEP_ALIVE_TIME
                    )
                except asyncio.TimeoutError:
                    # No reconnect context, then health check.
                    cxt = None

                if cxt is None:
                    if self._conn is None:
                        continue

                    # Health check.
                    if (
                        utils.timestamp() - self._last_active_time
                        <= self.KEEP_ALIVE_TIME
                    ):
                        continue

                    if await self._health_check():
                        continue
                    else:
                        # Health check failed. If shutdown, stop loop.
                        if self.is_shutdown():
                            break
                        # Set unhealthy, make reconnect context.
                        self.set_unhealthy()
                        cxt = ReconnectCxt()

                elif cxt.server_addr:
                    # Check server validity.
                    if not self._server_manager.is_server_valid(cxt.server_addr):
                        logger.debug("[%s] server is invalid: %s", cxt.server_addr)
                        cxt.server_addr = None

                await self._reconnect(cxt)

            except asyncio.CancelledError:
                # Stop loop when cancel task.
                break

            except Exception as err:
                logger.debug("[%s] switch server failed: %s", self._service, err)

    async def _conn_event_notify(self):
        # logger.debug("[%s] wait conn event.", self._name)

        while not self.is_shutdown():
            try:
                event = await self._conn_event_queue.get()
                if event.is_connected():
                    await self._notify_connected()
                elif event.is_disconnected():
                    await self._notify_disconnected()

            except asyncio.CancelledError:
                # Stop loop when cancel task.
                break
            except Exception as err:
                logger.error("[%s] conn event notify failed: %s", self._service, err)

    async def _reconnect(self, cxt: ReconnectCxt):
        if cxt.on_req_fail and await self._health_check():
            self.set_running()
            return

        # Close old connection.
        await self._close_connection()

        logger.debug(
            "[%s] reconnect server: %s", self._service, cxt.server_addr or "random"
        )

        # Loop until reconnect successfully.
        reconnect_times = 0
        retry_turns = 0
        reconnect_success = False
        while not reconnect_success and not self.is_shutdown():
            error = None
            try:
                server_addr = cxt.server_addr or self._server_manager.next_server()
                conn_new = await self._connect2server(server_addr)

                if conn_new:
                    # reconnect succeeded
                    logger.debug(
                        "[%s] reconnect server: %s, conn: %s",
                        self._service,
                        server_addr,
                        conn_new.conn_id,
                    )

                    # if self._connection:
                    #     await self._close_connection()

                    self._conn = conn_new
                    self.set_running()
                    reconnect_success = True
                    self._add_conn_event(ConnectionEvent.CONNECTED)
                    return

                # Close connection if shutdown after reconnecting.
                if self.is_shutdown() and self._conn:
                    logger.debug("[%s] grpc shutdown, close new conn.")
                    await self._close_connection()

            except Exception as err:
                error = err
                # Will retry new server.
                cxt.server_addr = None

            if self._server_manager.size() == 0:
                raise NacosException(
                    NacosException.CLIENT_ERR, msg="server list is empty."
                )

            if reconnect_times and reconnect_times % self._server_manager.size() == 0:
                logger.debug(
                    "[%s] reconnect failed %s times, server: %s, error: %s",
                    self._service,
                    reconnect_times,
                    self._server_manager.cur_server(),
                    error,
                )

                # 0x7fffffff, 2^31 - 1
                if retry_turns == 0x7FFFFFFF:
                    retry_turns = 50
                else:
                    retry_turns += 1

            reconnect_times += 1

            if not self.is_running():
                # First round, try servers at a delay 100ms
                # Second round, 200ms
                # Max delays 5s. to be reconsidered.
                await asyncio.sleep(min(retry_turns + 1, 50) * 0.1)

            if self.is_shutdown():
                logger.debug("[%s] grpc shutdown, stop reconnect.", self._service)

    async def _notify_connected(self):
        # logger.debug("[%s] notify connected.", self._name)
        self._server_request_task = asyncio.create_task(self._handle_server_request())

        for listener in self._connection_listeners:
            try:
                listener.on_connected()
            except Exception as err:
                logger.debug("[%s] notify connected failed: %s", self._service, err)

    async def _notify_disconnected(self):
        # logger.debug("[%s] notify disconnected.", self._name)
        if self._server_request_task and not self._server_request_task.done():
            self._server_request_task.cancel()
            self._server_request_task = None

        for listener in self._connection_listeners:
            try:
                listener.on_disconnected()
            except Exception as err:
                logger.debug("[%s] notify disconnected failed: %s", self._service, err)
