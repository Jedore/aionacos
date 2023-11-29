import asyncio
from typing import Callable, Dict, Tuple

from .log import logger


class RedoService(object):
    def __init__(self, service: str):
        self._service = service
        self._redo_funcs: Dict[str, Tuple[Callable, tuple, dict]] = {}

        # Do not redo when first on connected.
        self._is_first_connected = True

    def add(self, func: Callable, *args, **kwargs):
        key = f"{func.__name__}{args}{kwargs}"
        if key in self._redo_funcs:
            return

        self._redo_funcs[key] = (func, args, kwargs)

    def redo(self):
        if self._is_first_connected:
            self._is_first_connected = False
            return

        logger.info(
            "[%s] Redo service start, count: %s",
            self._service,
            len(self._redo_funcs),
        )

        for _, value in self._redo_funcs.items():
            func, args, kwargs = value
            if asyncio.iscoroutinefunction(func):
                asyncio.create_task(func(*args, **kwargs))
            else:
                func(*args, **kwargs)

        logger.info("[%s] Redo service stop.", self._service)
