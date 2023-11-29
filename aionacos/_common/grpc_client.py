import asyncio
import copy
import enum
import json
import typing as t

import grpc

from . import constants as cst
from .exceptions import NacosException, RemoteException
from .listener import ConnectionEventListener
from .log import logger
from .payload import PAYLOAD_REGISTRY
from .request import *
from .response import *
from .server_manager import ServerManager
from .server_request_handler import ServerRequestHandler
from .utils import NacosJSONEncoder
from .._protocol import *
from .._utils import local_ip, timestamp


class GrpcStatus(str, enum.Enum):
    def __str__(self):
        return self.value

    INITIALIZING = "GrpcClient is initializing..."
    STARTING = "GrpcClient is starting..."
    RUNNING = "GrpcClient is running..."
    UNHEALTHY = "GrpcClient is unhealthy"
    SHUTDOWN = "GrpcClient is shutdown"


class ClientDetectionRequestHandler(ServerRequestHandler):
    async def request_reply(self, req: Request):
        if isinstance(req, ClientDetectionRequest):
            return ClientDetectionResponse()


class ConnectResetRequestHandler(ServerRequestHandler):
    """
    Connect to new server addr by server requested.
    """

    def __init__(self, grpc_client: "GrpcClient"):
        super().__init__()

        self._grpc_client = grpc_client

    async def request_reply(self, req: Request):
        if isinstance(req, ConnectResetRequest):
            try:
                if self._grpc_client.is_running():
                    if req.serverIp:
                        server_addr = (
                            f"{req.serverIp}{cst.COLON}{req.serverPort}"
                        )
                        # todo do not wait
                        await self._grpc_client.switch_server_async(
                            server_addr=server_addr
                        )
                    else:
                        await self._grpc_client.switch_server_async()
            except Exception as err:
                logger.error(
                    "[%s] ConnectResetRequest failed: %s",
                    self._grpc_client.service,
                    err,
                )

            return ConnectResetResponse()


class ReconnectContext(object):
    def __init__(self, server_addr: str = None, on_request_fail: bool = True):
        self.server_addr = server_addr
        self.on_request_fail = on_request_fail


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
    """
    Connection between client(naming/config) and server.
    """

    def __init__(self, service: str, server_addr: str, param: int = 0):
        self._service = service
        self._server_addr = server_addr
        self._channel = grpc.aio.insecure_channel(
            self._server_addr,
            options=(("grpc.experimental.tcp_min_read_chunk_size", param),),
        )
        self._stub = RequestStub(self._channel)
        self._bi_stub = BiRequestStreamStub(self._channel)
        self._bi_stream = self._bi_stub.requestBiStream()
        self.connection_id: t.Optional[str] = None
        # self.abandon = False

    @property
    def server_addr(self):
        return self._server_addr

    @staticmethod
    def _convert_req(req: Request) -> Payload:
        """
        Convert request to payload.
        """
        _req = copy.deepcopy(req)
        payload = Payload()
        payload.metadata.clientIp = local_ip()
        payload.metadata.type = _req.cls_name
        payload.metadata.headers.update(_req.headers)
        _req.headers.clear()
        payload.body.value = json.dumps(_req, cls=NacosJSONEncoder).encode(
            cst.ENCODE
        )
        return payload

    @staticmethod
    def _convert_rsp(rsp: Response) -> Payload:
        """
        Convert response to payload.
        """
        payload = Payload()
        payload.metadata.type = rsp.cls_name
        payload.body.value = json.dumps(rsp, cls=NacosJSONEncoder).encode(
            cst.ENCODE
        )
        return payload

    def _parse(
        self, payload: Payload
    ) -> t.Optional[t.Union[ResponseType, RequestType]]:
        """
        Parse payload to request/response.
        """
        if not hasattr(payload, "body"):
            return

        try:
            data: dict = json.loads(payload.body.value)
        except json.JSONDecodeError as err:
            logger.error("[%s] Json decode failed: %s", self._service, err)
            raise NacosException(NacosException.SERVER_ERR, msg=str(err))

        # 'type' is builtin class, so use 'type_'
        if "type" in data:
            data["type_"] = data.pop("type")

        payload_type = PAYLOAD_REGISTRY.get(payload.metadata.type)
        if payload_type is None:
            logger.error(
                "[%s] Unknown payload, service type: %s, body: %s",
                self._service,
                payload.metadata.type,
                data,
            )
            raise RemoteException(
                NacosException.SERVER_ERR,
                msg="Unknown payload service type:" + payload.metadata.type,
            )
        return payload_type(**data)

    async def request(
        self, req: Request, timeout: float = cst.READ_TIMEOUT
    ) -> ResponseType:
        rsp = await self._stub.request(self._convert_req(req), timeout=timeout)
        return self._parse(rsp)

    async def bi_request(self, req: Request):
        payload = self._convert_req(req)
        await self._bi_stream.write(payload)

    async def bi_response(self, rsp: Response):
        payload = self._convert_rsp(rsp)
        await self._bi_stream.write(payload)

    async def read_server_request(self) -> t.Optional[RequestType]:
        payload = await self._bi_stream.read()
        if payload is grpc.aio.EOF:
            # The stream is end, new a stream.
            self._bi_stream = self._bi_stub.requestBiStream()
            return
        return self._parse(payload)

    async def close(self):
        if (
            self._channel
            and self._channel.get_state() != grpc.ChannelConnectivity.SHUTDOWN
        ):
            try:
                await self._channel.close()
            except Exception as err:
                logger.error(
                    "[%s] Close connection failed: %s", self._service, err
                )
            finally:
                self._channel = None


class GrpcClient(object):
    KEEP_ALIVE_TIME = 6  # second
    RETRY_TIMES = 3

    def __init__(
        self,
        service: str,
        server_manager: ServerManager,
        labels: dict,
    ):
        self._service = service
        self._server_manager = server_manager
        # todo check
        self._labels = labels
        self._connection: t.Optional[Connection] = None
        self._last_active_time: int = timestamp()  # second
        self._server_request_handlers: t.List[ServerRequestHandler] = []
        self._connection_event_listeners: t.List[ConnectionEventListener] = []
        self._switch_server_queue: asyncio.Queue[
            ReconnectContext
        ] = asyncio.Queue(maxsize=1)
        self._connection_event_queue: asyncio.Queue[
            ConnectionEvent
        ] = asyncio.Queue()

        self._status = GrpcStatus.INITIALIZING

        self._tasks: t.List[asyncio.Task] = []
        self._handle_server_request_task: t.Optional[asyncio.Task] = None

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
        logger.info("[%s] %s", self._service, self._status.value)

    def set_running(self):
        self._status = GrpcStatus.RUNNING
        logger.debug("[%s] %s", self._service, self._status)

    def set_unhealthy(self):
        self._status = GrpcStatus.UNHEALTHY
        logger.debug("[%s] %s", self._service, self._status)

    def set_shutdown(self):
        self._status = GrpcStatus.SHUTDOWN
        logger.info("[%s] %s", self._service, self._status)

    async def start(self):
        self.set_starting()

        # Register request handlers.
        self.register_request_handler(ClientDetectionRequestHandler())
        self.register_request_handler(ConnectResetRequestHandler(self))

        # Try to connect to the same server multi times.
        retry_times = self.RETRY_TIMES
        while retry_times:
            server_addr = None
            try:
                server_addr = self._server_manager.next_server()
                self._connection = await self._connect2server(server_addr)

                if self._connection is not None:
                    logger.debug(
                        "[%s] connect server succeed: %s, connection id: %s",
                        self._service,
                        server_addr,
                        self._connection.connection_id,
                    )
                    self._add_connection_event(ConnectionEvent.CONNECTED)
                    self.set_running()

                    # Jump out loop.
                    break
            except Exception as err:
                logger.debug(
                    "[%s] connect server failed, server: %s, error: %s",
                    self._service,
                    server_addr,
                    err,
                )

            # Minus 1 when connect failed.
            retry_times -= 1

        if self._connection is None:
            await self.switch_server_async()

        # Create background tasks.
        self._tasks.append(asyncio.create_task(self._switch_server_consumer()))
        self._tasks.append(
            asyncio.create_task(self._connection_event_consumer())
        )

    def stop(self):
        """
        Shutdown all tasks and connection.
        """
        self.set_shutdown()
        for task in self._tasks:
            if not task.done():
                task.cancel()
        self._tasks.clear()

        self._connection.close()

        logger.debug("[%s] Shutdown grpc client.", self._service)

    def _add_connection_event(self, event_type: int):
        self._connection_event_queue.put_nowait(ConnectionEvent(event_type))

    async def _server_check(self, connection: Connection) -> t.Optional[str]:
        """
        Check server and return connection id.
        """
        logger.debug(
            "[%s] Server check: %s",
            self._service,
            connection.server_addr,
        )

        error = None
        try:
            rsp = await connection.request(
                ServerCheckRequest(),
                cst.READ_TIMEOUT,
            )
            if not isinstance(rsp, ServerCheckResponse):
                await connection.close()
            elif not rsp.success:
                error = rsp.message
            else:
                # Check succeed.
                logger.debug(
                    "[%s] Server check succeed: %s",
                    self._service,
                    connection.server_addr,
                )
                return rsp.connectionId

        # except grpc.aio.AioRpcError as err:
        #     error = str(err)
        except Exception as err:
            error = str(err)

        logger.debug("[%s] Server check failed: %s", self._service, error)

    async def _connect2server(
        self, server_addr: str
    ) -> t.Optional[Connection]:
        logger.debug(
            "[%s] Connecting to server: %s",
            self._service,
            server_addr,
        )

        # 8192/8193 is for creating 2 different connection for naming and config.
        # todo find better way
        if self._service == "Naming":
            connection = Connection(self._service, server_addr, param=8192)
        else:
            connection = Connection(self._service, server_addr, param=8193)

        # Server check.
        connection.connection_id = await self._server_check(connection)

        # Check failed.
        if connection.connection_id is None:
            return

        # Set up connection.
        await connection.bi_request(
            ConnectionSetupRequest(
                labels=self._labels,
                clientVersion=cst.CLIENT_VERSION,
            )
        )

        # Wait to set up connection done.
        await asyncio.sleep(1)

        return connection

    async def _close_connection(self):
        if self._connection:
            logger.debug(
                "[%s] Close connection, server: %s, connection id: %s",
                self._service,
                self._connection.server_addr,
                self._connection.connection_id,
            )
            self.set_unhealthy()
            await self._connection.close()
            self._connection = None
            self._add_connection_event(ConnectionEvent.DISCONNECTED)

    def _set_active_time(self):
        self._last_active_time = timestamp()
        logger.debug("[%s] Set last active time", self._service)

    def register_request_handler(self, request_handler: ServerRequestHandler):
        self._server_request_handlers.append(request_handler)
        logger.debug(
            "[%s] Register server request handler: %s",
            self._service,
            request_handler.name,
        )

    def register_connection_listener(self, listener: ConnectionEventListener):
        self._connection_event_listeners.append(listener)
        logger.debug(
            "[%s] Register connection event listener: %s",
            self._service,
            listener.name,
        )

    async def request(
        self, req: Request, timeout: float = cst.READ_TIMEOUT
    ) -> ResponseType:
        # todo exception deal
        retry_times = 0
        t_start = timestamp()
        error = None
        while (
            retry_times < self.RETRY_TIMES and timestamp() < t_start + timeout
        ):
            wait_reconnect = False
            rsp = None
            try:
                if self._connection is None or not self.is_running():
                    wait_reconnect = True
                    # Raise error, then retry.
                    raise NacosException(
                        NacosException.CLIENT_DISCONNECT, msg=self._status
                    )

                rsp = await self._connection.request(req, timeout=timeout)

                # Error response.
                if isinstance(rsp, ErrorResponse):
                    if rsp.errorCode == NacosException.UN_REGISTER:
                        wait_reconnect = True
                        self.set_unhealthy()
                        await self.switch_server_async()

                    # Raise error, then retry.
                    raise NacosException(rsp.errorCode, msg=rsp.message)

                # Update active time.
                self._set_active_time()
                return rsp

            except Exception as err:
                # Wait before next retry loop when reconnecting...
                if wait_reconnect:
                    await asyncio.sleep(min(0.1, timeout / 3))

                logger.debug(
                    "[%s] Request failed, retry_times: %s, error: %s, req: %s, rsp: %s",
                    self._service,
                    retry_times,
                    err,
                    req,
                    rsp,
                )

                error = str(err)

            retry_times += 1

        # Switch server when always failed.
        if self.is_running():
            self.set_unhealthy()
            await self._switch_server_async_on_request_fail()

        # todo don't raise but return error
        # Raise request error.
        raise NacosException(NacosException.SERVER_ERR, msg=error)

    async def _health_check(self) -> bool:
        if self._connection is None:
            return False

        if timestamp() - self._last_active_time <= self.KEEP_ALIVE_TIME:
            return True

        logger.debug(
            "[%s] Health check: %s, %s",
            self._service,
            self._connection.server_addr,
            self._connection.connection_id,
        )

        rsp = None
        try:
            rsp = await self.request(HealthCheckRequest())
            if rsp and rsp.success:
                return True
            else:
                error = str(rsp)
        except Exception as err:
            error = str(err)

        logger.debug(
            "[%s] Health check failed, error: %s, rsp: %s",
            self._service,
            error,
            rsp,
        )
        return False

    async def _switch_server_async_on_request_fail(self):
        await self.switch_server_async(on_request_fail=True)

    async def switch_server_async(
        self, server_addr: str = None, on_request_fail: bool = False
    ):
        logger.debug(
            "[%s] Put item into switch server queue, server addr: %s, on_request_fail: %s",
            self._service,
            server_addr or "random",
            on_request_fail,
        )

        try:
            self._switch_server_queue.put_nowait(
                ReconnectContext(
                    server_addr=server_addr, on_request_fail=on_request_fail
                )
            )
        except asyncio.QueueFull:
            logger.debug(
                "[%s] Put item into switch server queue failed: queue full.",
                self._service,
                server_addr or "random",
                on_request_fail,
            )

    async def _handle_server_request(self):
        logger.debug("[%s] Start server push request consumer.", self._service)

        while self.is_running():
            try:
                req = await self._connection.read_server_request()

                logger.debug(
                    "[%s] Receive server push request: %s",
                    self._service,
                    req,
                )

                if req is None:
                    continue

                for handler in self._server_request_handlers:
                    rsp = await handler.request_reply(req)
                    if rsp is not None:
                        rsp.requestId = req.requestId
                        await self._connection.bi_response(rsp)
                        logger.debug(
                            "[%s] Ack server push request, request: %s, response: %s",
                            self._service,
                            req,
                            rsp,
                        )
                        # Handled succeed, skip other handlers.
                        break
            except grpc.aio.AioRpcError as err:
                logger.debug(
                    "[%s] Receive push request failed: %s, %s",
                    self._service,
                    err.code(),
                    err.details(),
                )
                # todo why
                await asyncio.sleep(self.KEEP_ALIVE_TIME)

            except asyncio.CancelledError:
                # Stop loop when cancel task.
                break
            except Exception as err:
                logger.debug(
                    "[%s] Handle server push request failed: %s",
                    self._service,
                    err,
                )

        logger.debug("[%s] Stop server push request consumer.", self._service)

    async def _switch_server_consumer(self):
        """
        Consume switch server async queue and Health check.
        """
        logger.debug(
            "[%s] Start switch server consumer and health check.",
            self._service,
        )

        while not self.is_shutdown():
            try:
                try:
                    # Get reconnect context
                    cxt: t.Optional[ReconnectContext] = await asyncio.wait_for(
                        self._switch_server_queue.get(), self.KEEP_ALIVE_TIME
                    )
                except asyncio.TimeoutError:
                    # No reconnect context, then health check.
                    cxt = None

                if cxt is None:
                    if self._connection is None:
                        continue

                    # Health check.
                    if (
                        timestamp() - self._last_active_time
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
                        cxt = ReconnectContext()

                elif cxt.server_addr:
                    # Check server validity.
                    if not self._server_manager.is_server_valid(
                        cxt.server_addr
                    ):
                        logger.debug(
                            "[%s] Recommend server is not in server list, ignored: %s",
                            cxt.server_addr,
                        )
                        cxt.server_addr = None

                await self._reconnect(cxt)

            except asyncio.CancelledError:
                # Stop loop when cancel task.
                break

            except Exception as err:
                logger.debug(
                    "[%s] Switch server failed: %s",
                    self._service,
                    str(err),
                )

        logger.debug("[%s] Stop switch server consumer.", self._service)

    async def _connection_event_consumer(self):
        """
        Consume connection event.
        """
        logger.debug("[%s] Start connection event consumer.", self._service)

        while not self.is_shutdown():
            try:
                event = await self._connection_event_queue.get()
                if event.is_connected():
                    await self._notify_connected()
                elif event.is_disconnected():
                    await self._notify_disconnected()

            except asyncio.CancelledError:
                # Stop loop when cancel task.
                break
            except Exception as e:
                logger.error(
                    "[%s] Consume connection event failed: %s",
                    self._service,
                    e,
                )

        logger.info("[%s] Stop connection event consumer.", self._service)

    async def _reconnect(self, cxt: ReconnectContext):
        """
        Reconnect to server.
        """
        if cxt.on_request_fail and await self._health_check():
            self.set_running()
            logger.debug(
                "[%s] Reconnect, health check succeed, server: %s",
                self._service,
                self._server_manager.cur_server(),
            )
            return

        # Close old connection.
        await self._close_connection()

        logger.debug(
            "[%s] Try to reconnect to a new server: %s",
            self._service,
            cxt.server_addr or "random",
        )

        # Loop until reconnect successfully.
        reconnect_times = 0
        retry_turns = 0
        switch_success = False
        while not switch_success and not self.is_shutdown():
            error = None
            try:
                server_addr = (
                    cxt.server_addr or self._server_manager.next_server()
                )
                connection_new = await self._connect2server(server_addr)

                if connection_new:
                    # reconnect succeeded
                    logger.debug(
                        "[%s] Reconnect server succeed: %s, connection id: %s",
                        self._service,
                        server_addr,
                        connection_new.connection_id,
                    )

                    if self._connection:
                        await self._close_connection()

                    self._connection = connection_new
                    self.set_running()
                    switch_success = True
                    self._add_connection_event(ConnectionEvent.CONNECTED)
                    return

                # Close connection if shutdown after reconnecting.
                if self.is_shutdown() and self._connection:
                    logger.warn(
                        "[%s] GrpcClient is shutdown, close new connection."
                    )
                    await self._close_connection()

            except Exception as err:
                error = err
                # Will retry new server.
                cxt.server_addr = None

            if self._server_manager.size() == 0:
                msg = "Reconnect, server list is empty."
                logger.debug("[%s] %s", self._service, msg)
                # Raise error when server list empty.
                raise NacosException(NacosException.CLIENT_ERR, msg=msg)

            if (
                reconnect_times
                and reconnect_times % self._server_manager.size() == 0
            ):
                logger.debug(
                    "[%s] Reconnect failed after trying %s times, last try server: %s, error: %s",
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
                logger.debug(
                    "[%s] GrpcClient is shutdown, stop reconnecting.",
                    self._service,
                )

    async def _notify_connected(self):
        logger.debug("[%s] Notify connected.", self._service)

        # Create handle server request task after connecting.
        self._handle_server_request_task = asyncio.create_task(
            self._handle_server_request()
        )

        for listener in self._connection_event_listeners:
            try:
                listener.on_connected()
            except Exception as err:
                logger.debug(
                    "[%s] Notify connection listener %s failed: %s",
                    self._service,
                    listener.name,
                    err,
                )

    async def _notify_disconnected(self):
        logger.debug("[%s] Notify disconnected.", self._service)

        # Cancel handle server request task.
        if (
            self._handle_server_request_task
            and not self._handle_server_request_task.done()
        ):
            self._handle_server_request_task.cancel()
            self._handle_server_request_task = None

        for listener in self._connection_event_listeners:
            try:
                listener.on_disconnected()
            except Exception as err:
                logger.debug(
                    "[%s] Notify disconnection listener %s failed: %s",
                    self._service,
                    listener.name,
                    err,
                )
