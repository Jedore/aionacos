import sys
import time

import netifaces


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
        return ""
