from .change_parser import CONFIG_PARSER_REGISTRY


class ConfigChangeHandler(object):
    @classmethod
    def parse_change_data(cls, old_content: str, new_content: str, type_: str):
        parser = CONFIG_PARSER_REGISTRY.get(type_.lower())
        if parser:
            return parser.do_parse(old_content, new_content)
