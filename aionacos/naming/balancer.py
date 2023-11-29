from aionacos.naming.pojo import ServiceInfo


class Balancer(object):
    @staticmethod
    def get_host_by_random_weight(service_info: ServiceInfo):
        if not service_info.hosts:
            # print('host size = 0')
            return
        # todo
