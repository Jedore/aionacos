import abc
import typing as t

import orjson
import yaml

from .event import ConfigChangeItem, ChangeType
from ..common.log import logger

CONFIG_PARSER_REGISTRY: t.Dict[str, "AbstractConfigChangeParser"] = {}


class ConfigChangeParserMeta(abc.ABCMeta):
    def __new__(metacls, cls_name, bases, cls_dict):
        new_cls = super().__new__(metacls, cls_name, bases, cls_dict)
        if cls_name != "AbstractConfigChangeParser":
            CONFIG_PARSER_REGISTRY[cls_dict["config_type"]] = new_cls()
        return new_cls


class AbstractConfigChangeParser(metaclass=ConfigChangeParserMeta):
    config_type = ""

    @abc.abstractmethod
    def parse(self, old_content: str, new_content: str):
        raise NotImplementedError('users must define "parse" to use this base class')

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


class TextConfigChangeParser(AbstractConfigChangeParser):
    config_type = "text"

    def parse(self, old_content: str, new_content: str):
        key, type_ = "content", None
        if not old_content and new_content:
            type_ = ChangeType.ADDED
        elif old_content and new_content and old_content != new_content:
            type_ = ChangeType.MODIFIED
        elif old_content and not new_content:
            type_ = ChangeType.DELETED

        return {
            "content": ConfigChangeItem(
                key=key,
                type_=type_,
                new_value=new_content,
                old_value=old_content,
            )
        }


class PropertiesConfigChangeParser(AbstractConfigChangeParser):
    config_type = "properties"

    def parse(self, old_content: str, new_content: str):
        # todo properties parser in future
        raise NotImplementedError('users must define "parse" to use this base class')


class YamlConfigChangeParser(AbstractConfigChangeParser):
    config_type = "yaml"

    def parse(self, old_content: str, new_content: str):
        try:
            old_map = yaml.load(old_content, yaml.Loader) if old_content else {}
            new_map = yaml.load(new_content, yaml.Loader) if new_content else {}
            return self.filter_change_data(old_map, new_map)
        except Exception as err:
            logger.error(f"[Config] Parse yaml failed: {err}")


class JsonConfigChangeParser(AbstractConfigChangeParser):
    config_type = "json"

    def parse(self, old_content: str, new_content: str):
        try:
            old_map = orjson.loads(old_content) if old_content else {}
            new_map = orjson.loads(new_content) if new_content else {}
            return self.filter_change_data(old_map, new_map)
        except Exception as err:
            logger.error(f"[Config] Parse json failed: {err}")


class XMLConfigChangeParser(AbstractConfigChangeParser):
    config_type = "xml"

    def parse(self, old_content: str, new_content: str):
        # todo xml parser in future
        raise NotImplementedError('users must define "parse" to use this base class')


class HTMLConfigChangeParser(AbstractConfigChangeParser):
    config_type = "html"

    def parse(self, old_content: str, new_content: str):
        # todo html parser in future
        raise NotImplementedError('users must define "parse" to use this base class')


def get_parser(config_type: str) -> AbstractConfigChangeParser:
    return CONFIG_PARSER_REGISTRY.get(config_type.lower(), TextConfigChangeParser)
