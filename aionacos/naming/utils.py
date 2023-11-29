import re
from typing import List

from .pojo import Instance
from .._common import constants as cst
from .._common.exceptions import NacosException


class NamingUtils(object):
    CLUSTER_NAME_PATTERN = re.compile(cst.CLUSTER_NAME_PATTERN_STRING)
    NUMBER_PATTERN = re.compile(cst.NUMBER_PATTERN_STRING)

    @classmethod
    def check_instance_legal(cls, ins: Instance):
        if (
            ins.heartbeat_timeout < ins.heartbeat_interval
            or ins.ip_del_timeout < ins.heartbeat_interval
        ):
            raise NacosException(NacosException.INVALID_PARAM)

        if ins.clusterName and not cls.CLUSTER_NAME_PATTERN.match(
            ins.clusterName
        ):
            raise NacosException(NacosException.INVALID_PARAM)

    @staticmethod
    def get_group_name(service_name: str, group_name: str):
        if not service_name:
            raise NacosException(
                NacosException.INVALID_PARAM, msg="service_name is invalid"
            )

        if not group_name:
            raise NacosException(
                NacosException.INVALID_PARAM, msg="group_name is invalid"
            )
        return group_name + cst.SERVICE_INFO_SPLITER + service_name

    @staticmethod
    def parse_clusters(clusters: List[str]):
        return ",".join(clusters) if clusters else ""


class Chooser(object):
    pass
