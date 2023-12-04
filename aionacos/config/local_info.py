from pathlib import Path

from ..common.constants import ENCODE
from ..common.log import logger


# todo whether use snapshot
def get_failover():
    pass


async def save_snapshot(
    data_id: str, group: str, tenant: str, cache_dir: Path, config: str = None
):
    # TODO How to avoid conflicting between multi instances.
    file = get_snapshot_file(data_id, group, tenant, cache_dir)

    if not config:
        file.unlink(missing_ok=True)
        return

    try:
        file.write_text(config, encoding=ENCODE)
    except Exception as err:
        logger.warning(f"[Config] store snapshot failed: {file}, {err}")


async def get_snapshot(data_id: str, group: str, tenant: str, cache_dir: Path):
    file = get_snapshot_file(data_id, group, tenant, cache_dir)
    if not file.exists() or not file.is_file():
        return None

    try:
        return file.read_text(encoding=ENCODE)
    except Exception as err:
        logger.warning(f"[Config] load snapshot failed: {file}, {err}")
        return None


def get_snapshot_file(data_id: str, group: str, tenant: str, cache_dir: Path):
    if not cache_dir.exists():
        cache_dir.mkdir(parents=True, exist_ok=True)
    # todo chinese in file name
    return cache_dir / f"{tenant}_{group}_{data_id}.snapshot"


def get_encrypt_failover():
    pass


async def save_encrypt_snapshot(
    data_id: str, group: str, tenant: str, cache_dir: Path, encrypt_data_key: str = None
):
    # TODO How to avoid conflicting between multi instances.
    file = get_encrypt_snapshot_file(data_id, group, tenant, cache_dir)

    if not encrypt_data_key:
        file.unlink(missing_ok=True)
        return

    try:
        file.write_text(encrypt_data_key, encoding=ENCODE)
    except Exception as err:
        logger.warning(f"[Config] store snapshot failed: {file}, {err}")


async def get_encrypt_snapshot(data_id: str, group: str, tenant: str, cache_dir: Path):
    file = get_encrypt_snapshot_file(data_id, group, tenant, cache_dir)
    if not file.exists() or not file.is_file():
        return None

    try:
        return file.read_text(encoding=ENCODE)
    except Exception as err:
        logger.warning(f"[Config] load snapshot failed: {file}, {err}")
        return None


def get_encrypt_snapshot_file(data_id: str, group: str, tenant: str, cache_dir: Path):
    if not cache_dir.exists():
        cache_dir.mkdir(parents=True, exist_ok=True)
    # todo chinese in file name
    return cache_dir / f"{tenant}_{group}_{data_id}.encrypt-data-key.snapshot"
