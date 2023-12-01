import pickle
from pathlib import Path

from .pojo import ServiceInfo
from .._common.log import logger


def make_sure_cache_dir_exists(cache_dir: Path):
    if not cache_dir.exists():
        cache_dir.mkdir(parents=True, exist_ok=True)


def write(service_info: ServiceInfo, cache_dir: Path):
    make_sure_cache_dir_exists(cache_dir)

    file = cache_dir / service_info.get_key_encoded()
    # TODO How to avoid conflicting between multi instances.
    file.write_bytes(pickle.dumps(service_info, protocol=pickle.HIGHEST_PROTOCOL))
    logger.debug(f"[Naming] store service info to {file}")


def read(cache_dir: Path):
    make_sure_cache_dir_exists(cache_dir)
    service_info_map = {}
    for path in cache_dir.iterdir():
        if not path.is_file():
            continue

        try:
            service_info = pickle.loads(path.read_bytes())
            service_info_map[service_info.get_key2()] = service_info
        except Exception as err:
            logger.warning(f"[Naming] load service info failed: {path}, {err}")
    logger.debug("[Naming] load service info")
    return service_info_map
