# =============================================================================
# Casanova Named Record Helper
# =============================================================================
#
# CSV-aware improvement over python's namedtuple.
#
from json import dumps
from collections import OrderedDict

from casanova._namedtuple import future_namedtuple


def namedrecord(name, fields, boolean=None, plural=None, json=None, defaults=None):
    mapping = {k: i for i, k in enumerate(fields)}
    mask = []

    for k in fields:
        if boolean and k in boolean:
            mask.append(1)
        elif plural and k in plural:
            mask.append(2)
        elif json and k in json:
            mask.append(3)
        else:
            mask.append(0)

    def cast_for_csv(i, v, plural_separator='|'):
        if v is None:
            return None

        m = mask[i]

        if m == 0:
            return v

        if m == 1:
            return 'true' if v else 'false'

        if m == 2:
            assert isinstance(v, list)
            return plural_separator.join(str(i) for i in v)

        if m == 3:
            return dumps(v, ensure_ascii=False)

        raise TypeError

    class Record(future_namedtuple(name, fields, defaults=defaults)):
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

        def as_csv_row(self, plural_separator='|'):
            row = list(
                cast_for_csv(i, v, plural_separator=plural_separator)
                for i, v in enumerate(self)
            )

            return row

        def as_csv_dict_row(self, plural_separator='|'):
            row = OrderedDict(
                (fields[i], cast_for_csv(i, v, plural_separator=plural_separator))
                for i, v in enumerate(self)
            )

            return row

        def as_dict(self):
            return {fields[i]: v for i, v in enumerate(self)}

    Record.__name__ = name
    Record.fieldnames = list(fields)

    return Record
