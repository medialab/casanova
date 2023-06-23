# =============================================================================
# Casanova Writer
# =============================================================================
#
# A CSV writer that is only really useful if you intend to resume its operation
# somehow
#
from typing import Optional, Iterable, Iterator, Mapping
from casanova.types import AnyCSVDialect, AnyWritableCSVRowPart

import csv

from casanova.defaults import DEFAULTS
from casanova.serialization import CSVSerializer, CustomTypes
from casanova.resumers import Resumer, BasicResumer, LastCellResumer
from casanova.namedrecord import (
    coerce_row,
    coerce_fieldnames,
    AnyFieldnames,
    infer_fieldnames,
)
from casanova.reader import Headers
from casanova.utils import py310_wrap_csv_writerow, strip_null_bytes_from_row
from casanova.exceptions import InconsistentRowTypesError, InvalidRowTypeError


class Writer(object):
    __supported_resumers__ = (
        BasicResumer,
        LastCellResumer,
    )

    def __init__(
        self,
        output_file,
        fieldnames: Optional[AnyFieldnames] = None,
        row_len: Optional[int] = None,
        strip_null_bytes_on_write: Optional[bool] = None,
        dialect: Optional[AnyCSVDialect] = None,
        delimiter: Optional[str] = None,
        quotechar: Optional[str] = None,
        quoting: Optional[int] = None,
        escapechar: Optional[str] = None,
        lineterminator: Optional[str] = None,
        write_header: bool = True,
        strict: bool = True,
    ):
        if strip_null_bytes_on_write is None:
            strip_null_bytes_on_write = DEFAULTS.strip_null_bytes_on_write

        if not isinstance(strip_null_bytes_on_write, bool):
            raise TypeError('expecting a boolean as "strip_null_bytes_on_write" kwarg')

        self.strip_null_bytes_on_write = strip_null_bytes_on_write

        no_headers = fieldnames is None

        self.fieldnames = coerce_fieldnames(fieldnames) if not no_headers else None
        self.headers = Headers(self.fieldnames) if not no_headers else None
        self.no_headers = no_headers
        self.row_len = None

        if row_len is not None:
            self.row_len = row_len
        elif self.fieldnames is not None:
            self.row_len = len(self.fieldnames)

        self.strict = strict and self.row_len is not None

        self.resuming = False

        if isinstance(output_file, Resumer):
            if self.no_headers:
                raise NotImplementedError

            resumer = output_file

            if not isinstance(output_file, self.__class__.__supported_resumers__):
                raise TypeError(
                    "%s: does not support %s!"
                    % (self.__class__.__name__, output_file.__class__.__name__)
                )

            self.resuming = resumer.can_resume()

            if self.resuming:
                # NOTE: how about null bytes
                resumer.get_insights_from_output(
                    self,
                    no_headers=self.no_headers,
                    dialect=dialect,
                    quotechar=quotechar,
                    delimiter=delimiter,
                )

            output_file = resumer.open_output_file()

        # Instantiating writer
        self.dialect = dialect
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.escapechar = escapechar
        self.quoting = quoting
        self.lineterminator = lineterminator

        writer_kwargs = {}

        if dialect is not None:
            writer_kwargs["dialect"] = dialect

        if delimiter is not None:
            writer_kwargs["delimiter"] = delimiter

        if quotechar is not None:
            writer_kwargs["quotechar"] = quotechar

        if escapechar is not None:
            writer_kwargs["escapechar"] = escapechar

        if quoting is not None:
            writer_kwargs["quoting"] = quoting

        if lineterminator is not None:
            writer_kwargs["lineterminator"] = lineterminator

        self.__writer = csv.writer(output_file, **writer_kwargs)

        if not strip_null_bytes_on_write:
            self._writerow = py310_wrap_csv_writerow(self.__writer)
        else:
            self._writerow = lambda row: self.__writer.writerow(
                strip_null_bytes_from_row(row)
            )

        self.should_write_header = not self.resuming and self.fieldnames is not None

        if self.should_write_header and write_header:
            self.writeheader()

    def writerow(
        self, row: AnyWritableCSVRowPart, *parts: AnyWritableCSVRowPart
    ) -> None:
        has_multiple_parts = len(parts) > 0
        row = coerce_row(row, consume=has_multiple_parts)

        for part in parts:
            row.extend(coerce_row(part))

        if self.strict and len(row) != self.row_len:
            raise TypeError(
                "casanova.writer.writerow: expected %i cells but got %i."
                % (self.row_len, len(row))
            )

        self._writerow(row)

    def writerows(self, rows: Iterable[AnyWritableCSVRowPart]) -> None:
        for row in rows:
            self.writerow(row)

    def writeheader(self) -> None:
        if self.fieldnames is None:
            raise TypeError("cannot write header if fieldnames were not provided")

        self.should_write_header = False
        self._writerow(self.fieldnames)


class InferringWriter(Writer):
    __supported_resumers__ = (
        BasicResumer,
        LastCellResumer,
    )

    def __init__(
        self,
        output_file,
        fieldnames: Optional[AnyFieldnames] = None,
        add: Optional[AnyFieldnames] = None,
        plural_separator: Optional[str] = None,
        none_value: Optional[str] = None,
        true_value: Optional[str] = None,
        false_value: Optional[str] = None,
        stringify_everything: Optional[bool] = None,
        custom_types: Optional[CustomTypes] = None,
        **kwargs
    ):
        self.added_fieldnames = None
        self.added_count = 0

        if add is not None:
            self.added_fieldnames = coerce_fieldnames(add)
            self.added_count = len(self.added_fieldnames)

        super().__init__(
            output_file, fieldnames=fieldnames, write_header=False, **kwargs
        )

        # Own serializer
        self.serializer = CSVSerializer(
            plural_separator=plural_separator,
            none_value=none_value,
            true_value=true_value,
            false_value=false_value,
            stringify_everything=stringify_everything,
            custom_types=custom_types,
        )

        # Lifecycle
        self.__must_infer = self.fieldnames is None

        if self.resuming:
            self.__must_infer = False
        elif self.fieldnames is not None:
            self.writeheader()

    # NOTE: this could be more DRY wrt Writer inheritance
    def writeheader(self) -> None:
        if self.fieldnames is None:
            raise TypeError("cannot write header if fieldnames were not provided")

        self.should_write_header = False

        row = self.fieldnames

        if self.added_fieldnames is not None:
            row = row + self.added_fieldnames

        self._writerow(row)

    def __set_fieldnames(self, fieldnames):
        fieldnames = coerce_fieldnames(fieldnames)

        self.fieldnames = fieldnames
        self.headers = Headers(fieldnames)
        self.row_len = len(fieldnames)

        self.__must_infer = False

    def writerow(self, data, add: Optional[AnyWritableCSVRowPart] = None) -> None:
        if isinstance(data, (Iterator, range)):
            data = list(data)

        if self.__must_infer:
            fieldnames = infer_fieldnames(data)

            if fieldnames is None:
                raise InvalidRowTypeError(
                    'given data type "%s" cannot be safely cast to a tabular row (e.g. a set has no defined order)'
                    % data.__class__.__name__
                )

            self.__set_fieldnames(fieldnames)
            self.writeheader()

        # NOTE: coercing after inferrence not to lose fieldnames info
        __csv_row__ = getattr(data, "__csv_row__", None)

        if callable(__csv_row__):
            data = __csv_row__()

        if isinstance(data, Mapping):
            row = [self.serializer(data.get(k)) for k in self.fieldnames]
        elif isinstance(data, (list, tuple)):
            row = [self.serializer(v) for v in data]
        else:
            row = [self.serializer(data)]

        if len(row) != self.row_len:
            raise InconsistentRowTypesError

        if add is not None:
            if self.added_fieldnames is None:
                raise TypeError("not expecting additional information")

            add = coerce_row(add)

            if len(add) != self.added_count:
                raise TypeError("inconsistent addition len")

            row += add

        self._writerow(row)
