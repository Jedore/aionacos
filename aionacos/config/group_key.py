from urllib.parse import quote_plus

PLUS = "+"

PERCENT = "%"

TWO = "2"

B = "B"

FIVE = "5"


# def get_key(data_id: str, group: str, datum: str = ''):
#     return do_get_key(data_id, group, datum)


def get_key_tenant(data_id: str, group: str, tenant: str):
    return do_get_key(data_id, group, tenant)


def do_get_key(data_id: str, group: str, datum: str):
    sb = f"{quote_plus(data_id)}{PLUS}{quote_plus(group)}"
    if datum:
        sb = f"{sb}{PLUS}{quote_plus(datum)}"
    return sb


def parse_key():
    pass
