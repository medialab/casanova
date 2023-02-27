# =============================================================================
# Casanova Named Record Helper
# =============================================================================
#
# CSV-aware improvement over python's namedtuple.
#
from typing import Optional, Iterable

from collections import OrderedDict, namedtuple
from dataclasses import fields

from casanova.serialization import CSVSerializer

DEFAULT = 0
JSON = 1


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

    serializer = CSVSerializer(
        plural_separator=plural_separator,
        none_value=none_value,
        true_value=true_value,
        false_value=false_value,
    )

    class Record(namedtuple(name, fields, defaults=defaults)):
        _is_namedrecord = True

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
                serializer(
                    v,
                    plural_separator=plural_separator,
                    none_value=none_value,
                    true_value=true_value,
                    false_value=false_value,
                    as_json=mask[i] == JSON,
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
                    serializer(
                        v,
                        plural_separator=plural_separator,
                        none_value=none_value,
                        true_value=true_value,
                        false_value=false_value,
                        as_json=mask[i] == JSON,
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


TABULAR_RECORD_SERIALIZER = CSVSerializer()


class TabularRecord(object):
    _serializer_options = {
        "plural_separator": "|",
        "none_value": "",
        "true_value": "true",
        "false_value": "false",
        "stringify_everything": True,
    }

    def as_csv_row(self):
        return [
            TABULAR_RECORD_SERIALIZER(
                getattr(self, field.name), **self._serializer_options
            )
            for field in fields(self)
        ]

    def as_csv_dict_row(self):
        return {
            field.name: TABULAR_RECORD_SERIALIZER(
                getattr(self, field.name), **self._serializer_options
            )
            for field in fields(self)
        }
