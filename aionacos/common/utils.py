from json import JSONEncoder


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


class NacosJSONEncoder(JSONEncoder):
    def default(self, o: any) -> any:
        if isinstance(o, Serializable):
            return o.dict()
        return super().default(o)
