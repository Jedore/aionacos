from .change_parser import *


class ConfigChangeHandler(object):
    parsers = (
        JsonChangeParser(),
        PropertiesChangeParser(),
        YamlChangeParser(),
    )

    @classmethod
    def parse_change_data(cls, old_content: str, new_content: str, type_: str):
        for parser in cls.parsers:
            if parser.is_responsible_for(type_):
                return parser.do_parse(old_content, new_content)
