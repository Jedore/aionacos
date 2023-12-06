import sys
import time
from hashlib import md5

import netifaces

from . import constants as cst


class Serializable(object):
    """
    Custom serializable class for request/event .etc
    """

    def dict(self):
        data = {}
        for attr in self.__slots__:  # noqa
            # if attr == 'headers':
            #     continue
            value = getattr(self, attr, None)
            # if value is not None and value.__class__.__name__ in ('Instance',):
            #     value = value.dict()
            data[attr] = value
        return data

    def __str__(self):
        return self.__class__.__name__ + str(self.dict())

    def __repr__(self):
        return self.__class__.__name__ + str(self.dict())


def timestamp():
    """second timestamp"""
    return round(time.time())


def timestamp_milli():
    """second timestamp"""
    return round(1000 * time.time())


def local_ip() -> str:
    try:
        if sys.platform.startswith("linux"):
            addr = netifaces.ifaddresses("eth0")
        elif sys.platform.startswith("win32"):
            for item in netifaces.interfaces():
                if "Intel" in item:
                    addr = netifaces.ifaddresses(item)
                    break
            else:
                return ""
        elif sys.platform.startswith("darwin"):
            addr = netifaces.ifaddresses("en0")
        else:
            return ""
        return addr.get(netifaces.AF_INET)[0].get("addr")
    except Exception as e:  # noqa
        # todo error
        return ""


def default(o: any) -> dict:
    if isinstance(o, Serializable):
        return o.dict()
    raise TypeError


def md5_hex(value: str, encode: str):
    if value is None:
        return cst.NULL
    return md5(value.encode(encode)).hexdigest()
