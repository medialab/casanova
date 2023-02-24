# =============================================================================
# Casanova Named Record Helper
# =============================================================================
#
# CSV-aware improvement over python's namedtuple.
#
from typing import Optional, Iterable

from json import dumps
from collections import OrderedDict, namedtuple

from casanova.defaults import DEFAULTS

DEFAULT = 0
JSON = 1


def cast_for_csv(
    mask: int,
    value: str,
    plural_separator: str,
    none_value: str,
    true_value: str,
    false_value: str,
):
    if mask == JSON:
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


# NOTE: boolean & plural are just indicative and don't serve any purpose
# anymore but to be additional metadata that could be useful later on
# NOTE: json could also become indicative only at one point
def namedrecord(
    name: str,
    fields: Iterable[str],
    boolean=None,
    plural=None,
    json: Optional[Iterable[str]] = None,
    defaults: Optional[Iterable] = None,
    plural_separator: Optional[str] = None,
    none_value: Optional[str] = None,
    true_value: Optional[str] = None,
    false_value: Optional[str] = None,
):
    fields = list(fields)

    mapping = {k: i for i, k in enumerate(fields)}
    mask = []

    json = list(json) if json is not None else None

    for k in fields:
        if json and k in json:
            mask.append(JSON)
        else:
            mask.append(DEFAULT)

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
        ):
            row = list(
                cast_for_csv(
                    mask[i],
                    v,
                    plural_separator=plural_separator,
                    none_value=none_value,
                    true_value=true_value,
                    false_value=false_value,
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
                    ),
                )
                for i, v in enumerate(self)
            )

            return row

        def as_dict(self):
            return {fields[i]: v for i, v in enumerate(self)}

    Record.__name__ = name
    Record.fieldnames = fields.copy()
    Record.boolean = list(boolean) if boolean is not None else None
    Record.plural = list(plural) if plural is not None else None
    Record.json = json

    return Record
