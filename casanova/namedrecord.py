# =============================================================================
# Casanova Named Record Helper
# =============================================================================
#
# CSV-aware improvement over python's namedtuple.
#
import sys
import json
from typing import Optional, Iterable, Union

if sys.version_info[:2] >= (3, 10):
    from typing import get_origin, get_args
else:
    from typing_extensions import get_origin, get_args

from collections import OrderedDict, namedtuple
from dataclasses import fields, field

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
    Record.is_namedrecord = True
    Record.fieldnames = fields.copy()
    Record.boolean = list(boolean) if boolean is not None else None
    Record.plural = list(plural) if plural is not None else None
    Record.json = json

    return Record


TABULAR_RECORD_SERIALIZER = CSVSerializer()
TABULAR_FIELDS = {}


def tabular_field(
    *,
    plural_separator: Optional[str] = None,
    none_value: Optional[str] = None,
    true_value: Optional[str] = None,
    false_value: Optional[str] = None,
    stringify_everything: Optional[bool] = None,
    as_json: Optional[bool] = None,
    **field_kwargs
):
    f = field(**field_kwargs)

    f_serialization_options = {}

    if plural_separator is not None:
        f_serialization_options["plural_separator"] = plural_separator

    if none_value is not None:
        f_serialization_options["none_value"] = none_value

    if true_value is not None:
        f_serialization_options["true_value"] = true_value

    if false_value is not None:
        f_serialization_options["false_value"] = false_value

    if stringify_everything is not None:
        f_serialization_options["stringify_everything"] = stringify_everything

    if as_json is not None and as_json:
        f_serialization_options["as_json"] = as_json

    if f_serialization_options:
        TABULAR_FIELDS[f] = f_serialization_options

    return f


NoneType = type(None)
PluralTypes = (list, set, tuple)


# TODO: deal with straightforward unions (Literal notably)
def parse(
    string,
    t,
    plural_separator: str = "|",
    none_value: str = "",
    true_value: str = "true",
    as_json: bool = False,
):
    if as_json:
        return json.loads(string)

    if t is str:
        return string

    if t is bool:
        return string == true_value

    if t is int:
        return int(string)

    if t is float:
        return float(string)

    origin = get_origin(t)

    if origin is None:
        raise NotImplementedError("cannot parse because type origin is unknown")

    args = get_args(t)

    if origin in PluralTypes:
        values = (
            parse(s, args[0], none_value=none_value, true_value=true_value)
            for s in string.split(plural_separator)
        )

        return origin(values)

    if origin is Union:
        if len(args) > 2 or args[1] is not NoneType:
            raise NotImplementedError(
                "cannot parse arbitrary Union except for Optional"
            )

        if string == none_value:
            return None

        return parse(
            string,
            args[0],
            plural_separator=plural_separator,
            none_value=none_value,
            true_value=true_value,
        )

    raise NotImplementedError("cannot parse arbitrary types")


class TabularRecord(object):
    _serializer_options = {
        "plural_separator": "|",
        "none_value": "",
        "true_value": "true",
        "false_value": "false",
        "stringify_everything": True,
    }

    @classmethod
    def get_fieldnames(cls):
        return [f.name for f in fields(cls)]

    @classmethod
    def parse(cls, row):
        parsed = []
        fs = fields(cls)

        if len(row) != len(fs):
            raise TypeError(
                "attempting to parse a row with wrong number of items (%i while expecting %i)"
                % (len(row), len(fs))
            )

        options = cls._serializer_options

        for v, f in zip(row, fs):
            f_options = {**options, **TABULAR_FIELDS.get(f, {})}

            parsed.append(
                parse(
                    v,
                    f.type,
                    plural_separator=f_options["plural_separator"],
                    none_value=f_options["none_value"],
                    true_value=f_options["true_value"],
                    as_json=f_options.get("as_json", False),
                )
            )

        return cls(*parsed)

    def as_csv_row(self):
        row = []

        options = self._serializer_options

        for f in fields(self):
            f_options = {**options, **TABULAR_FIELDS.get(f, {})}

            row.append(TABULAR_RECORD_SERIALIZER(getattr(self, f.name), **f_options))

        return row

    def as_csv_dict_row(self):
        row = {}

        options = self._serializer_options

        for f in fields(self):
            f_options = {**options, **TABULAR_FIELDS.get(f, {})}

            row[f.name] = TABULAR_RECORD_SERIALIZER(getattr(self, f.name), **f_options)

        return row


def coerce_row(row, consume=False):
    as_csv_row = getattr(row, "as_csv_row", None)

    if callable(as_csv_row):
        row = as_csv_row()

    return list(row) if consume else row


def is_tabular_record_class(cls) -> bool:
    try:
        return issubclass(cls, TabularRecord)
    except TypeError:
        return False


def coerce_fieldnames(cls):
    if is_tabular_record_class(cls):
        return cls.get_fieldnames()

    if getattr(cls, "is_namedrecord", False):
        return cls.fieldnames

    return cls
