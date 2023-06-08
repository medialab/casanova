# =============================================================================
# Casanova Named Record Helper
# =============================================================================
#
# CSV-aware improvement over python's namedtuple.
#
import sys
import json
from typing import Optional, Iterable, Union, List, Callable, Any, Type
from casanova.types import AnyWritableCSVRowPart

if sys.version_info[:2] >= (3, 10):
    from typing import get_origin, get_args
else:
    from typing_extensions import get_origin, get_args

from collections import OrderedDict, namedtuple
from collections.abc import Mapping
from dataclasses import fields, field, Field

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

        def __csv_row__(self):
            return self.as_csv_row()

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

        def __csv_dict_row__(self):
            return self.as_csv_dict_row()

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
    serializer: Optional[Callable[[Any], str]] = None,
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

    if serializer is not None:
        f_serialization_options["serializer"] = serializer

    if f_serialization_options:
        TABULAR_FIELDS[f] = f_serialization_options

    return f


NoneType = type(None)
PluralTypes = (list, frozenset, set, tuple)


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


def _cached_fields(cls):
    fs = cls._cached_fields

    if fs is None:
        fs = fields(cls)
        cls._cached_fields = fs

    return fs


class TabularRecord(object):
    _cached_fields = None
    _serializer_options = {
        "plural_separator": "|",
        "none_value": "",
        "true_value": "true",
        "false_value": "false",
        "stringify_everything": True,
    }

    @classmethod
    def fieldnames(cls, prefix: str = ""):
        names = []

        for f in _cached_fields(cls):
            if is_tabular_record_class(f.type):
                names.extend(f.type.fieldnames(prefix=prefix + f.name + "_"))
            else:
                names.append(prefix + f.name)

        return names

    @classmethod
    def parse(cls, row, _offset=0):
        parsed = []
        fs = _cached_fields(cls)

        options = cls._serializer_options

        i = _offset

        while i < len(row):
            v = row[i]
            f = fs[i - _offset]

            f_options = {**options, **TABULAR_FIELDS.get(f, {})}

            if is_tabular_record_class(f.type):
                i, sub_record = f.type.parse(row, _offset=i)
                parsed.append(sub_record)
            else:
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
                i += 1

        try:
            record = cls(*parsed)
        except TypeError:
            raise TypeError(
                "mismatch between length of rows and number of properties held by tabular record"
            )

        if _offset > 0:
            return i, record

        return record

    def as_csv_row(self):
        row = []

        options = self._serializer_options

        for f in _cached_fields(self):
            f_options = {**options, **TABULAR_FIELDS.get(f, {})}

            if is_tabular_record_class(f.type):
                row.extend(getattr(self, f.name).as_csv_row())
            else:
                custom_serializer = f_options.get("serializer")
                v = getattr(self, f.name)

                if custom_serializer is not None:
                    s = custom_serializer(v)
                else:
                    s = TABULAR_RECORD_SERIALIZER(v, **f_options)

                row.append(s)

        return row

    def __csv_row__(self):
        return self.as_csv_row()

    def as_csv_dict_row(self):
        row = {}

        options = self._serializer_options

        for f in _cached_fields(self):
            f_options = {**options, **TABULAR_FIELDS.get(f, {})}

            if is_tabular_record_class(f.type):
                data = getattr(self, f.name).as_csv_dict_row()

                for n, v in data.items():
                    row[f.name + "_" + n] = v
            else:
                custom_serializer = f_options.get("serializer")
                v = getattr(self, f.name)

                if custom_serializer is not None:
                    s = custom_serializer(v)
                else:
                    s = TABULAR_RECORD_SERIALIZER(v, **f_options)

                row[f.name] = s

        return row

    def __csv_dict_row__(self):
        return self.as_csv_dict_row()


def coerce_row(row: AnyWritableCSVRowPart, consume: bool = False) -> List[Any]:
    if isinstance(row, (bytes, str)):
        raise TypeError("row parts should not be strings")

    __csv_row__ = getattr(row, "__csv_row__", None)

    if callable(__csv_row__):
        return __csv_row__()

    return list(row) if consume else row


def is_tabular_record_class(cls) -> bool:
    try:
        return issubclass(cls, TabularRecord)
    except TypeError:
        return False


def tabular_fields(cls) -> List[Field]:
    fs = []

    for f in _cached_fields(cls):
        if is_tabular_record_class(f.type):
            fs.extend(tabular_fields(f.type))
        else:
            fs.append(f)

    return fs


AnyFieldnames = Union[List[str], Type[TabularRecord]]


def coerce_fieldnames(target: AnyFieldnames) -> List[str]:
    if is_tabular_record_class(target):
        return target.fieldnames()

    if getattr(target, "is_namedrecord", False):
        return target.fieldnames

    return target


def infer_fieldnames(target: Any) -> Optional[List[str]]:
    if isinstance(target, TabularRecord):
        return target.__class__.fieldnames()

    if getattr(target.__class__, "is_namedrecord", False):
        return target.__class__.fieldnames

    if isinstance(target, Mapping):
        return list(target.keys())

    if isinstance(target, (list, tuple)):
        return ["col%i" % n for n in range(1, len(target) + 1)]

    if isinstance(target, (str, bytes, float, int, bool)) or target is None:
        return ["value"]

    return None
