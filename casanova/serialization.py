from typing import Optional

from json import dumps


class CSVSerializer(object):
    def __init__(
        self,
        plural_separator: Optional[str] = None,
        none_value: Optional[str] = None,
        true_value: Optional[str] = None,
        false_value: Optional[str] = None,
    ):
        self.plural_separator = plural_separator or "|"
        self.none_value = none_value or ""
        self.true_value = true_value or "true"
        self.false_value = false_value or "false"

    def __call__(
        self,
        value,
        plural_separator: Optional[str] = None,
        none_value: Optional[str] = None,
        true_value: Optional[str] = None,
        false_value: Optional[str] = None,
        as_json: bool = False,
        stringify_everything: bool = False,
    ):
        if as_json:
            return dumps(value, ensure_ascii=False)

        if value is None:
            none_value = none_value if none_value is not None else self.none_value

            if not none_value and not stringify_everything:
                return None

            return none_value

        if isinstance(value, str):
            return value

        if isinstance(value, bool):
            if value:
                return true_value if true_value is not None else self.true_value

            return false_value if false_value is not None else self.false_value

        if isinstance(value, (int, float)):
            if stringify_everything:
                return str(value)
            else:
                return value

        if isinstance(value, (list, set, frozenset, tuple)):
            plural_separator = (
                plural_separator
                if plural_separator is not None
                else self.plural_separator
            )
            return plural_separator.join(str(i) for i in value)

        raise NotImplementedError(
            "CSVSerializer does not support this kind of value: {}".format(
                value.__class__.__name__
            )
        )
