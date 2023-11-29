class AbstractSelector(object):
    __slots__ = ('type',)

    def __init__(self, type_: str):
        self.type = type_
