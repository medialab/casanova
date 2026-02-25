# =============================================================================
# Casanova Writer
# =============================================================================
#
# A CSV writer that is only really useful if you intend to resume its operation
# somehow
#
from typing import Optional, Iterable, Iterator, Mapping, TypeVar, Tuple
from casanova.types import AnyCSVDialect, AnyWritableCSVRowPart

import csv
from os import PathLike
from io import IOBase
from dataclasses import fields, is_dataclass
from collections import OrderedDict

from casanova.defaults import DEFAULTS
from casanova.serialization import CSVSerializer, CustomTypes
from casanova.resumers import Resumer, BasicResumer, LastCellResumer
from casanova.record import (
    coerce_row,
    coerce_fieldnames,
    AnyFieldnames,
    infer_fieldnames,
)
from casanova.reader import Headers
from casanova.utils import py310_wrap_csv_writerow, strip_null_bytes_from_row, ensure_open
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
        encoding: str = "utf-8",
        open_mode: str = 'w',
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
            # NOTE: basic resumer does not need to know the headers
            # TODO: at one point header knowledge could be refactored
            # as Resumer interface
            if self.no_headers and not isinstance(output_file, BasicResumer):
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

            output_type = "resumer"
            output_file = resumer.open_output_file()
        elif isinstance(output_file, IOBase):
            output_type = "file"
            output_file = output_file
        elif isinstance(output_file, (str, PathLike)):
            output_type = "path"
            output_file = ensure_open(output_file, mode=open_mode, encoding=encoding)
        else:
            raise TypeError("expecting a file or a path")

        # Instantiating writer
        self.output_type = output_type
        self.output_file = output_file
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

        self.__writer = csv.writer(self.output_file, **writer_kwargs)

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

    def close(self):
        if self.output_type != "path":
            raise NotImplementedError("cannot close a file this instance did not open")

        if self.output_file is not None:
            self.output_file.close()
        else:
            raise TypeError("no file to close")
        
    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.output_type == "path":
            self.close()


T = TypeVar("T")


def get(l: Tuple[T, ...], i: int) -> Optional[T]:
    try:
        return l[i]
    except IndexError:
        return


class InferringWriter(Writer):
    __supported_resumers__ = (
        BasicResumer,
        LastCellResumer,
    )

    def __init__(
        self,
        output_file,
        fieldnames: Optional[AnyFieldnames] = None,
        prepend: Optional[AnyFieldnames] = None,
        append: Optional[AnyFieldnames] = None,
        plural_separator: Optional[str] = None,
        none_value: Optional[str] = None,
        true_value: Optional[str] = None,
        false_value: Optional[str] = None,
        stringify_everything: Optional[bool] = None,
        custom_types: Optional[CustomTypes] = None,
        buffer_optionals: bool = False,
        mapping_sample_size: Optional[int] = None,
        strip_null_bytes_on_write: Optional[bool] = None,
        dialect: Optional[AnyCSVDialect] = None,
        delimiter: Optional[str] = None,
        quotechar: Optional[str] = None,
        quoting: Optional[int] = None,
        escapechar: Optional[str] = None,
        lineterminator: Optional[str] = None,
    ):
        self.prepended_fieldnames = None
        self.prepended_count = 0

        if prepend is not None:
            self.prepended_fieldnames = coerce_fieldnames(prepend)
            self.prepended_count = len(self.prepended_fieldnames)

        self.appended_fieldnames = None
        self.appended_count = 0

        if append is not None:
            self.appended_fieldnames = coerce_fieldnames(append)
            self.appended_count = len(self.appended_fieldnames)

        super().__init__(
            output_file,
            fieldnames=fieldnames,
            write_header=False,
            strip_null_bytes_on_write=strip_null_bytes_on_write,
            dialect=dialect,
            delimiter=delimiter,
            quotechar=quotechar,
            quoting=quoting,
            escapechar=escapechar,
            lineterminator=lineterminator,
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

        # Optionals buffering
        self.buffering_optionals = buffer_optionals

        # Mapping samples
        if mapping_sample_size is not None and mapping_sample_size < 1:
            raise TypeError("mapping_sample_size should be > 0")

        self.mapping_sample_size = mapping_sample_size
        self.__mappings_sampled = 0
        self.__union_of_keys = OrderedDict()

        self.__buffer_flushed = False
        self.__buffer = []

        # Lifecycle
        # NOTE: we must infer even when resuming to ensure
        # row len consistency etc.
        self.__must_infer = self.fieldnames is None

        if not self.resuming and self.fieldnames is not None:
            self.writeheader()

    # NOTE: this could be more DRY wrt Writer inheritance
    def writeheader(self) -> None:
        if self.fieldnames is None:
            raise TypeError("cannot write header if fieldnames were not provided")

        self.should_write_header = False

        row = self.fieldnames

        if self.prepended_fieldnames is not None:
            row = self.prepended_fieldnames + row

        if self.appended_fieldnames is not None:
            row = row + self.appended_fieldnames

        self._writerow(row)

    def __set_fieldnames(self, fieldnames):
        fieldnames = coerce_fieldnames(fieldnames)

        self.fieldnames = fieldnames
        self.headers = Headers(fieldnames)
        self.row_len = len(fieldnames)

        self.__must_infer = False

    def __flush_buffer(self) -> None:
        if self.__buffer_flushed:
            return

        # NOTE: this is important to avoid endless recursion
        self.__buffer_flushed = True

        for p in self.__buffer:
            self.writerow(*p)

        self.__buffer.clear()

    def close(self) -> None:
        if not self.__buffer_flushed and self.__buffer and self.__must_infer:
            if self.mapping_sample_size is not None and self.__mappings_sampled > 0:
                self.__set_fieldnames(list(self.__union_of_keys.keys()))
            elif self.buffering_optionals:
                self.__set_fieldnames(["col1"])

            self.writeheader()
            self.__flush_buffer()

    def __del__(self):
        self.close()

    def writerow(self, *parts) -> None:
        data = None
        prepend = None
        append = None

        if self.prepended_fieldnames is None:
            if self.appended_fieldnames is None:
                data = get(parts, 0)
            else:
                data = get(parts, 0)
                append = get(parts, 1)
        else:
            if self.appended_fieldnames is None:
                prepend = get(parts, 0)
                data = get(parts, 1)
            else:
                prepend = get(parts, 0)
                data = get(parts, 1)
                append = get(parts, 2)

        if isinstance(data, (Iterator, range)):
            data = list(data)

        if self.__must_infer:
            if self.buffering_optionals and data is None:
                self.__buffer.append(parts)
                return

            fieldnames = infer_fieldnames(data)

            if fieldnames is None:
                raise InvalidRowTypeError(
                    'given data type "%s" cannot be safely cast to a tabular row (e.g. a set has no defined order)'
                    % data.__class__.__name__
                )

            if self.mapping_sample_size is not None and isinstance(data, Mapping):
                if self.__mappings_sampled < self.mapping_sample_size:
                    self.__buffer.append(parts)
                    self.__mappings_sampled += 1

                    for k in data.keys():
                        self.__union_of_keys[k] = True

                    return

                fieldnames = list(self.__union_of_keys.keys())

            self.__set_fieldnames(fieldnames)
            self.writeheader()

        self.__flush_buffer()

        # NOTE: coercing after inferrence not to lose fieldnames info
        __csv_row__ = getattr(data, "__csv_row__", None)

        if callable(__csv_row__):
            data = __csv_row__()

        elif is_dataclass(data):
            data = [getattr(data, f.name) for f in fields(data)]

        elif self.buffering_optionals and data is None:
            data = [None] * self.row_len

        if isinstance(data, Mapping):
            row = [self.serializer(data.get(k)) for k in self.fieldnames]
        elif isinstance(data, (list, tuple)):
            row = [self.serializer(v) for v in data]
        else:
            row = [self.serializer(data)]

        if len(row) != self.row_len:
            raise InconsistentRowTypesError

        if self.prepended_fieldnames is not None:
            if prepend is not None:
                prepend = coerce_row(prepend)

                if len(prepend) != self.prepended_count:
                    raise TypeError("inconsistent prepend len")
            else:
                prepend = [None] * self.prepended_count

            row = prepend + row
        elif prepend is not None:
            raise TypeError("not expecting prepended information")

        if self.appended_fieldnames is not None:
            if append is not None:
                append = coerce_row(append)

                if len(append) != self.appended_count:
                    raise TypeError("inconsistent append len")
            else:
                append = [None] * self.appended_count

            row += append
        elif append is not None:
            raise TypeError("not expecting appended information")

        self._writerow(row)
