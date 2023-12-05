import json
from abc import ABCMeta

from .event import ConfigChangeItem, ChangeType


# todo use metadata like payload registry


class AbstractConfigChangeParse(metaclass=ABCMeta):
    config_type = ""

    @classmethod
    def is_responsible_for(cls, config_type: str):
        """Ignore case"""
        return cls.config_type.lower() == config_type.lower()

    @staticmethod
    def do_parse(old_content: str, new_content: str):
        raise NotImplementedError()

    @staticmethod
    def filter_change_data(old_config: dict, new_config: dict):
        changed_map = {}
        for key, old_value in old_config.items():
            if key not in new_config:
                item = ConfigChangeItem(
                    key=key, old_value=old_value, type_=ChangeType.DELETED
                )
                changed_map[key] = item
            else:
                new_value = new_config.get(key)
                if old_value == new_value:
                    continue
                item = ConfigChangeItem(
                    key=key,
                    old_value=old_value,
                    new_value=new_value,
                    type_=ChangeType.MODIFIED,
                )
                changed_map[key] = item

        for key, new_value in new_config.items():
            if key not in old_config:
                item = ConfigChangeItem(
                    key=key, new_value=new_value, type_=ChangeType.ADDED
                )
                changed_map[key] = item
        return changed_map


class PropertiesChangeParser(AbstractConfigChangeParse):
    config_type = "properties"

    @classmethod
    def do_parse(cls, old_content: str, new_content: str):
        pass


class YamlChangeParser(AbstractConfigChangeParse):
    config_type = "yaml"

    @classmethod
    def do_parse(cls, old_content: str, new_content: str):
        pass


class JsonChangeParser(AbstractConfigChangeParse):
    config_type = "json"

    @classmethod
    def do_parse(cls, old_content: str, new_content: str):
        old_props = json.loads(old_content) if old_content else {}
        new_props = json.loads(new_content) if new_content else {}

        return cls.filter_change_data(old_props, new_props)
