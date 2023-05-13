from typing import Union, List, Tuple, Dict, Iterator, KeysView, ValuesView, Any

import sys
import csv

if sys.version_info >= (3, 8):
    from typing import Protocol
else:
    from typing_extensions import Protocol

AnyCSVDialect = Union[str, csv.Dialect]


class CSVWritable(Protocol):
    def __csv_row__(self) -> List[Any]:
        ...


class CSVDictWritable(Protocol):
    def __csv_dict_row__(self) -> Dict[str, Any]:
        ...


AnyWritableCSVRowPart = Union[
    CSVWritable, Iterator[Any], List[Any], KeysView[Any], ValuesView[Any], Tuple[Any]
]
