import os

from .pojo import ServiceInfo
from .._common import properties


class DiskCache(object):
    @classmethod
    def make_sure_cache_dir_exists(cls, cache_dir: str):
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

    @classmethod
    def write(cls, service_info: ServiceInfo):
        cls.make_sure_cache_dir_exists(properties.cache_dir)

        file_path = os.path.join(
            properties.cache_dir, service_info.get_key_encoded()
        )
        # TODO How to avoid conflicting between multi instances.
        with open(file_path) as fo:
            # todo
            pass
