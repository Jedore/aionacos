from .pojo import ServiceInfo
from .utils import Chooser
from ..common.log import logger


class Balancer(object):
    @staticmethod
    def random_host_by_weight(service_info: ServiceInfo):
        if not service_info.hosts:
            logger.debug("[Naming] No host to serve for %s", service_info.name)
            return

        return Chooser.random_by_weight(service_info.hosts)
