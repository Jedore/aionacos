from hashlib import md5

from ..common import constants


def md5_hex(value: str, encode: str):
    if value is None:
        return constants.NULL
    return md5(value.encode(encode)).hexdigest()
