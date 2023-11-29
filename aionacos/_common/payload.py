import dis
from inspect import getmro
from itertools import tee

PAYLOAD_REGISTRY = {}


class SlotsMeta(type):
    """
    Automatic __slots__
    https://github.com/cjrh/autoslot
    """

    def __new__(metacls, cls_name, bases, cls_dict):
        slots = set()
        if "__init__" in cls_dict:
            init = cls_dict["__init__"]
            instructions = dis.Bytecode(init)
            i0, i1 = tee(instructions)
            for a, _ in zip(i0, i1):
                if a.opname == "STORE_ATTR":
                    slots.add(a.argval)

            for b in bases:
                for s in getmro(b):
                    slots |= getattr(s, "__slots__", set())
        cls_dict["__slots__"] = slots
        new_cls = super().__new__(metacls, cls_name, bases, cls_dict)
        return new_cls


class RegistryMeta(SlotsMeta):
    def __new__(metacls, cls_name, bases, cls_dict):
        new_cls = super().__new__(metacls, cls_name, bases, cls_dict)
        PAYLOAD_REGISTRY[cls_name] = new_cls
        return new_cls
