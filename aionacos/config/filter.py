from abc import ABCMeta
from typing import List


class ConfigRequest(object):
    __slots__ = ("param", "configContext")

    def __init__(self):
        pass


class ConfigResponse(object):
    __slots__ = (
        "dataId",
        "group",
        "content",
        "encryptedDataKey",
        "tenant",
        "configType",
    )

    def __init__(
        self,
        dataId: str = "",  # noqa
        group: str = "",
        content: str = "",
        configType: str = "",  # noqa
        tenant: str = "",
        encryptedDataKey: str = "",  # noqa
    ):
        self.dataId = dataId
        self.group = group
        self.content = content
        self.configType = configType
        self.tenant = tenant
        self.encryptedDataKey = encryptedDataKey


class ConfigFilterChain(metaclass=ABCMeta):
    def do_filter(self, req: ConfigRequest, rsp: ConfigResponse):
        raise NotImplementedError()


class ConfigFilter(metaclass=ABCMeta):
    def init(self, properties: dict):
        raise NotImplementedError()

    def do_filter(
        self, req: ConfigRequest, rsp: ConfigResponse, chain: ConfigFilterChain
    ):
        raise NotImplementedError()

    def get_order(self):
        raise NotImplementedError()

    def get_name(self):
        raise NotImplementedError()


class VirtualFilterChain(ConfigFilterChain):
    def __init__(self, additional_filters: List[ConfigFilter]):
        self.cur_position = 0
        self.additional_filters = additional_filters

    def do_filter(self, req: ConfigRequest, rsp: ConfigResponse):
        if self.cur_position != len(self.additional_filters):
            next_filter = self.additional_filters[self.cur_position]
            next_filter.do_filter(req, rsp, self)
            self.cur_position += 1


class ConfigFilterChainManager(ConfigFilterChain):
    def __init__(self):
        self.filters = []  # type: List[ConfigFilter]

    def add_filter(self, new_filter: ConfigFilter):
        for index, cur_filter in enumerate(self.filters):
            if cur_filter.get_name() == new_filter.get_name():
                break

            if new_filter.get_order() < cur_filter.get_order():
                self.filters.insert(index, new_filter)
                break
        else:
            self.filters.append(new_filter)

        return self

    def do_filter(self, req: ConfigRequest, rsp: ConfigResponse):
        VirtualFilterChain(self.filters).do_filter(req, rsp)


class ConfigEncryptionFilter(ConfigFilter):
    def init(self, properties: dict):
        pass

    def do_filter(
        self, req: ConfigRequest, rsp: ConfigResponse, chain: ConfigFilterChain
    ):
        pass

    def get_order(self):
        return 0

    def get_name(self):
        return self.__class__.__name__
