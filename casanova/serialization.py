from typing import Optional, Iterable

from json import dumps

from casanova.defaults import DEFAULTS


class CSVSerializer(object):
    def __init__(
        self,
        plural_separator: Optional[str] = None,
        none_value: Optional[str] = None,
        true_value: Optional[str] = None,
        false_value: Optional[str] = None,
    ):
        self.plural_separator = plural_separator
        self.none_value = none_value
        self.true_value = true_value
        self.false_value = false_value

    def serialize_value(
        self,
        value,
        plural_separator: Optional[str],
        none_value: Optional[str],
        true_value: Optional[str],
        false_value: Optional[str],
        as_json: bool = False,
    ):
        if as_json:
            return dumps(value, ensure_ascii=False)

        if value is None:
            none_value = none_value if none_value is not None else DEFAULTS.none_value

            return none_value

        if isinstance(value, str):
            return value

        if isinstance(value, bool):
            if value:
                return true_value if true_value is not None else DEFAULTS.true_value

            return false_value if false_value is not None else DEFAULTS.false_value

        if isinstance(value, Iterable):
            plural_separator = (
                plural_separator
                if plural_separator is not None
                else DEFAULTS.plural_separator
            )
            return plural_separator.join(str(i) for i in value)

        raise NotImplementedError
