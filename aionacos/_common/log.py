import logging

from . import properties


def get_logger():
    _logger = logging.getLogger("nacos")
    handler = logging.StreamHandler()
    fmt = logging.Formatter(
        "{asctime} | {levelname: <7}| {message}",
        style="{",
    )
    handler.setFormatter(fmt)
    _logger.addHandler(handler)
    _logger.propagate = False
    _logger.setLevel(properties.log_level)
    return _logger


logger = get_logger()
