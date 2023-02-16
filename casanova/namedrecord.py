# =============================================================================
# Casanova Named Record Helper
# =============================================================================
#
# CSV-aware improvement over python's namedtuple.
#
from json import dumps
from collections import OrderedDict, namedtuple
from collections.abc import Iterable

# from casanova.defaults import DEFAULTS

STRING = 0
BOOL = 1
PLURAL = 2
JSON = 3


def cast_for_csv(
    m: int,
    v: str,
    p: str = "|",
    n: str = "",
    t: str = "true",
    f: str = "false",
    i: bool = False,
):
    if v is None:
        return n

    if m == STRING:
        return v

    if m == BOOL:
        if v:
            return t

        if i:
            return ""

        return f

    if m == PLURAL:
        assert isinstance(v, Iterable)
        return p.join(str(i) for i in v)

    if m == JSON:
        return dumps(v, ensure_ascii=False)

    raise TypeError


def namedrecord(name, fields, boolean=None, plural=None, json=None, defaults=None):
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

        def as_csv_row(self, plural_separator="|"):
            row = list(
                cast_for_csv(mask[i], v, p=plural_separator) for i, v in enumerate(self)
            )

            return row

        def as_csv_dict_row(self, plural_separator="|"):
            row = OrderedDict(
                (fields[i], cast_for_csv(mask[i], v, p=plural_separator))
                for i, v in enumerate(self)
            )

            return row

        def as_dict(self):
            return {fields[i]: v for i, v in enumerate(self)}

    Record.__name__ = name
    Record.fieldnames = list(fields)

    return Record
