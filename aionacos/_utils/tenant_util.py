from os import getenv

from .._common import property_key_const as pkc

TENANT_ID = "tenant.id"

DEFAULT_ACM_NAMESPACE = ""

USER_TENANT = getenv(TENANT_ID, "")

ACM_NAMESPACE_PROPERTY = "acm.namespace"


def get_user_tenant_for_acm():
    if not USER_TENANT:
        return getenv(ACM_NAMESPACE_PROPERTY, DEFAULT_ACM_NAMESPACE)
    return USER_TENANT


def get_user_tenant_for_ans():
    if not USER_TENANT:
        return getenv(pkc.ANS_NAMESPACE)
    return USER_TENANT
