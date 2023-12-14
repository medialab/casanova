# =============================================================================
# Casanova Named Record Helper
# =============================================================================
#
# CSV-aware improvement over python's namedtuple.
#
from typing import Optional, Union, List, Callable, Any, Type, Dict, TypeVar, Tuple
from casanova.types import (
    AnyWritableCSVRowPart,
    get_args,
    get_origin,
    TypeGuard,
    Self,
    IsDataclass,
)

import json
from collections.abc import Mapping
from dataclasses import fields, field, Field, is_dataclass

from casanova.serialization import CSVSerializer

TABULAR_RECORD_SERIALIZER = CSVSerializer()

# NOTE: keeping a mention to CSV data in the method is still relevant because we
# are in fact serializing to a flavor of string. We could add row casting methods
# also for tabular format that are not CSV (e.g. ndjson)


def tabular_field(
    *,
    plural_separator: Optional[str] = None,
    none_value: Optional[str] = None,
    true_value: Optional[str] = None,
    false_value: Optional[str] = None,
    stringify_everything: Optional[bool] = None,
    as_json: Optional[bool] = None,
    serializer: Optional[Callable[[Any], str]] = None,
    **field_kwargs,
):
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

    metadata = field_kwargs.get("metadata", {})
    metadata["serialization_options"] = f_serialization_options
    field_kwargs["metadata"] = metadata

    return field(**field_kwargs)


NoneType = type(None)
PluralTypes = (list, frozenset, set, tuple)


# TODO: deal with straightforward unions (Literal notably)
def parse_value(
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
            parse_value(s, args[0], none_value=none_value, true_value=true_value)
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

        return parse_value(
            string,
            args[0],
            plural_separator=plural_separator,
            none_value=none_value,
            true_value=true_value,
        )

    raise NotImplementedError("cannot parse arbitrary types")


C = TypeVar("C", bound="TabularRecord")


def parse(cls: Type[C], row, offset=0) -> Tuple[int, C]:
    parsed = []
    fs = _cached_fields(cls)

    options = cls._serializer_options

    i = offset

    while i < len(row):
        v = row[i]
        f = fs[i - offset]

        f_options = {**options, **f.metadata.get("serialization_options", {})}

        if is_tabular_record_class(f.type):
            i, sub_record = parse(f.type, row, offset=i)
            parsed.append(sub_record)
        else:
            parsed.append(
                parse_value(
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

    if offset > 0:
        return i, record

    return 0, record


def _cached_fields(cls):
    fs = cls._cached_fields

    if fs is None:
        fs = fields(cls)
        cls._cached_fields = fs

    return fs


class TabularRecord:
    _cached_fields = None
    _serializer_options = {
        "plural_separator": "|",
        "none_value": "",
        "true_value": "true",
        "false_value": "false",
        "stringify_everything": True,
    }

    @classmethod
    def fieldnames(cls, prefix: str = "") -> List[str]:
        names = []

        for f in _cached_fields(cls):
            if is_tabular_record_class(f.type):
                names.extend(f.type.fieldnames(prefix=prefix + f.name + "_"))
            else:
                names.append(prefix + f.name)

        return names

    @classmethod
    def parse(cls, row) -> Self:
        return parse(cls, row)[1]

    def as_csv_row(self) -> List:
        row = []

        options = self._serializer_options

        for f in _cached_fields(self):
            f_options = {**options, **f.metadata.get("serialization_options", {})}

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

    def __csv_row__(self) -> List:
        return self.as_csv_row()

    def as_csv_dict_row(self) -> Dict[str, Any]:
        row = {}

        options = self._serializer_options

        for f in _cached_fields(self):
            f_options = {**options, **f.metadata.get("serialization_options", {})}

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

    def __csv_dict_row__(self) -> Dict[str, Any]:
        return self.as_csv_dict_row()


def coerce_row(row: AnyWritableCSVRowPart, consume: bool = False) -> List[Any]:
    if isinstance(row, (bytes, str)):
        raise TypeError("row parts should not be strings")

    __csv_row__ = getattr(row, "__csv_row__", None)

    if callable(__csv_row__):
        return __csv_row__()

    if is_dataclass(row):
        return [TABULAR_RECORD_SERIALIZER(getattr(row, f.name)) for f in fields(row)]

    return list(row) if consume else row  # type: ignore


def is_tabular_record_class(cls) -> TypeGuard[Type[TabularRecord]]:
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


AnyFieldnames = Union[List[str], Type[TabularRecord], Type[IsDataclass]]


def coerce_fieldnames(target: AnyFieldnames) -> List[str]:
    if is_tabular_record_class(target):
        return target.fieldnames()

    if is_dataclass(target):
        return [f.name for f in fields(target)]

    return target  # type: ignore


def infer_fieldnames(target: Any) -> Optional[List[str]]:
    if isinstance(target, TabularRecord):
        return target.__class__.fieldnames()

    if is_dataclass(target):
        return [f.name for f in fields(target)]

    if isinstance(target, Mapping):
        return list(target.keys())

    if isinstance(target, (list, tuple)):
        return ["col%i" % n for n in range(1, len(target) + 1)]

    if isinstance(target, (str, bytes, float, int, bool)) or target is None:
        return ["value"]

    return None
