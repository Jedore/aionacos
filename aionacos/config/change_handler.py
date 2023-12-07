from .change_parser import get_parser


class ConfigChangeHandler(object):
    @classmethod
    def parse_change_data(cls, old_content: str, new_content: str, type_: str):
        return get_parser(type_).parse(old_content, new_content)
