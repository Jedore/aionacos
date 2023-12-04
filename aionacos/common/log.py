import logging

from . import conf


def get_logger(prefix: str = ""):
    _logger = logging.getLogger("nacos")
    handler = logging.StreamHandler()
    fmt = logging.Formatter("{asctime} | {levelname: <7} | {message}", style="{")
    handler.setFormatter(fmt)
    _logger.addHandler(handler)
    _logger.propagate = False
    _logger.setLevel(conf.log_level)
    return _logger


logger = get_logger()
