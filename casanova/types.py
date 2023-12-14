from typing import (
    Union,
    List,
    Tuple,
    Dict,
    Iterator,
    KeysView,
    ValuesView,
    Any,
    ClassVar,
)

import sys
import csv

if sys.version_info >= (3, 8):
    from typing import Protocol
else:
    from typing_extensions import Protocol

if sys.version_info >= (3, 10):
    from typing import TypeGuard, get_args, get_origin
else:
    from typing_extensions import TypeGuard, get_args, get_origin

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

AnyCSVDialect = Union[str, csv.Dialect]


class CSVWritable(Protocol):
    def __csv_row__(self) -> List[Any]:
        ...


class CSVDictWritable(Protocol):
    def __csv_dict_row__(self) -> Dict[str, Any]:
        ...


class IsDataclass(Protocol):
    __dataclass_fields__ = ClassVar[Dict]


AnyWritableCSVRowPart = Union[
    CSVWritable,
    Iterator[Any],
    List[Any],
    KeysView[Any],
    ValuesView[Any],
    Tuple[Any, ...],
    IsDataclass,
]
