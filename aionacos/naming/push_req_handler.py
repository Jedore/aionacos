from .request import NotifySubscriberRequest
from .response import NotifySubscriberResponse
from .service_info_holder import ServiceInfoHolder
from ..common.request import Request
from ..common.server_req_handler import ServerRequestHandler


class NamingPushRequestHandler(ServerRequestHandler):
    def __init__(self, service_info_holder: ServiceInfoHolder):
        self._service_info_holder = service_info_holder

    async def request_reply(self, req: Request):
        if isinstance(req, NotifySubscriberRequest):
            self._service_info_holder.process_service(req.serviceInfo)
            return NotifySubscriberResponse()
