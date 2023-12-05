import logging
from os import getenv
from pathlib import Path

base_dir = Path(__file__).absolute().parent.parent

server_addr = getenv("NACOS_SERVER_ADDR", "localhost:8848")
encode = getenv("NACOS_ENCODE", "utf8")
log_level = logging.getLevelName(getenv("NACOS_LOG_LEVEL", "DEBUG"))
cache_dir = getenv("NACOS_CACHE_DIR")
# todo home dir
cache_dir = Path(cache_dir) if cache_dir else base_dir / ".nacos"

auth_enable = getenv("NACOS_AUTH_ENABLE") == "true"
username = getenv("NACOS_USERNAME")
password = getenv("NACOS_PASSWORD")

config_namespace = getenv("NACOS_CONFIG_NAMESPACE", "")
naming_namespace = getenv("NACOS_NAMING_NAMESPACE", "")

config_group = getenv("NACOS_CONFIG_GROUP", "")
naming_group = getenv("NACOS_NAMING_GROUP", "")

naming_load_cache_at_start = (
    getenv("NACOS_NAMING_LOAD_CACHE_AT_START", "true") == "true"
)
naming_push_empty_protection = (
    getenv("NACOS_NAMING_PUSH_EMPTY_PROTECTION", "true") == "true"
)

config_init_snapshot = getenv("NACOS_CONFIG_INIT_SNAPSHOT", "true") == "true"
