# =============================================================================
# Casanova Named Record Helper
# =============================================================================
#
# CSV-aware improvement over python's namedtuple.
#
from json import dumps
from collections import OrderedDict, namedtuple
from collections.abc import Iterable

from casanova.defaults import DEFAULTS

STRING = 0
BOOL = 1
PLURAL = 2
JSON = 3


def cast_for_csv(
    mask: int,
    value: str,
    plural_separator: str,
    none_value: str,
    true_value: str,
    false_value: str,
    ignore_false: bool,
):
    if value is None:
        none_value = none_value if none_value is not None else DEFAULTS.none_value

        return none_value

    if mask == STRING:
        return value

    if mask == BOOL:
        if value:
            return true_value if true_value is not None else DEFAULTS.true_value

        ignore_false = (
            ignore_false if ignore_false is not None else DEFAULTS.ignore_false
        )

        if ignore_false:
            return ""

        return false_value if false_value is not None else DEFAULTS.false_value

    if mask == PLURAL:
        assert isinstance(value, Iterable)
        plural_separator = (
            plural_separator
            if plural_separator is not None
            else DEFAULTS.plural_separator
        )
        return plural_separator.join(str(i) for i in value)

    else:
        return dumps(value, ensure_ascii=False)


def namedrecord(
    name,
    fields,
    boolean=None,
    plural=None,
    json=None,
    defaults=None,
    plural_separator=None,
    none_value=None,
    true_value=None,
    false_value=None,
    ignore_false=None,
):
    mapping = {k: i for i, k in enumerate(fields)}
    mask = []

    for k in fields:
        if boolean and k in boolean:
            mask.append(BOOL)
        elif plural and k in plural:
            mask.append(PLURAL)
        elif json and k in json:
            mask.append(JSON)
        else:
            mask.append(STRING)

    class Record(namedtuple(name, fields, defaults=defaults)):
        def __getitem__(self, key):
            if isinstance(key, str):
                idx = mapping.get(key)

                if idx is None:
                    raise KeyError

                return super().__getitem__(idx)

            return super().__getitem__(key)

        def get(self, key, default=None):
            try:
                return self.__getitem__(key)
            except (IndexError, KeyError):
                return default

        # NOTE: mind shadowing
        def as_csv_row(
            self,
            plural_separator=plural_separator,
            none_value=none_value,
            true_value=true_value,
            false_value=false_value,
            ignore_false=ignore_false,
        ):
            row = list(
                cast_for_csv(
                    mask[i],
                    v,
                    plural_separator=plural_separator,
                    none_value=none_value,
                    true_value=true_value,
                    false_value=false_value,
                    ignore_false=ignore_false,
                )
                for i, v in enumerate(self)
            )

            return row

        # NOTE: mind shadowing
        def as_csv_dict_row(
            self,
            plural_separator=plural_separator,
            none_value=none_value,
            true_value=true_value,
            false_value=false_value,
            ignore_false=ignore_false,
        ):
            row = OrderedDict(
                (
                    fields[i],
                    cast_for_csv(
                        mask[i],
                        v,
                        plural_separator=plural_separator,
                        none_value=none_value,
                        true_value=true_value,
                        false_value=false_value,
                        ignore_false=ignore_false,
                    ),
                )
                for i, v in enumerate(self)
            )

            return row

        def as_dict(self):
            return {fields[i]: v for i, v in enumerate(self)}

    Record.__name__ = name
    Record.fieldnames = list(fields)

    return Record
