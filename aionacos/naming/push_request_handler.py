from .request import NotifySubscriberRequest
from .response import NotifySubscriberResponse
from .service_info_holder import ServiceInfoHolder
from .._common.request import Request
from .._common.server_request_handler import ServerRequestHandler


class NamingPushRequestHandler(ServerRequestHandler):
    __slots__ = ("_server_info_holder",)

    def __init__(self, service_info_holder: ServiceInfoHolder):
        super().__init__()
        self._service_info_holder = service_info_holder

    async def request_reply(self, req: Request):
        if isinstance(req, NotifySubscriberRequest):
            self._service_info_holder.process_service_info(req.serviceInfo)
            return NotifySubscriberResponse()
