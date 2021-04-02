# =============================================================================
# Casanova Named Record Helper
# =============================================================================
#
# CSV-aware improvement over python's namedtuple.
#
from collections import OrderedDict, namedtuple


def namedrecord(name, fields, boolean=None, plural=None):
    mapping = OrderedDict((k, i) for i, k in enumerate(fields))

    mask = []

    for k in mapping.keys():
        if boolean and k in boolean:
            mask.append(1)
        elif plural and k in plural:
            mask.append(2)
        else:
            mask.append(0)

    def cast_for_csv(i, v, plural_separator='|'):
        m = mask[i]

        if m == 1:
            return 'true' if v else 'false'

        if m == 2:
            return plural_separator.join(v)

        return v

    class Record(namedtuple(name, fields)):
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

        def as_dict(self):
            return {fields[i]: v for i, v in enumerate(self)}

        def __repr__(self):
            representation = '<' + name

            for key, i in mapping.items():
                v = super().__getitem__(i)

                if isinstance(v, str) and len(v) > 30:
                    v = v[:29] + '…'

                representation += ' {k}={v!r}'.format(k=key, v=v)

            representation += '>'

            return representation

    return Record
