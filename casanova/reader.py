# =============================================================================
# Casanova Reader
# =============================================================================
#
# A fast but comfortable CSV reader based upon csv.reader to avoid dealing
# with csv.DictReader which is nice but very slow.
#
from typing import Optional, Iterator, List
from casanova.types import AnyCSVDialect

import csv
from os import PathLike
from collections import namedtuple
from collections.abc import Iterable
from itertools import chain
from io import IOBase, TextIOWrapper
from ebbe import without_last

from casanova.defaults import DEFAULTS
from casanova.headers import Headers
from casanova.namedrecord import is_tabular_record_class
from casanova.utils import (
    ensure_open,
    suppress_BOM,
    size_of_row_in_file,
    lines_without_null_bytes,
    rows_without_null_bytes,
    create_csv_aware_backwards_lines_iterator,
    ltpy311_csv_reader,
    looks_like_url,
    infer_delimiter_or_type,
)
from casanova.http import request
from casanova.exceptions import MissingColumnError, NoHeadersError

Multiplexer = namedtuple(
    "Multiplexer", ["column", "separator", "new_column"], defaults=["|", None]
)


class Reader(object):
    namespace = "casanova.reader"

    def __init__(
        self,
        input_file,
        no_headers: bool = False,
        encoding: str = "utf-8",
        dialect: Optional[AnyCSVDialect] = None,
        quotechar: Optional[str] = None,
        delimiter: Optional[str] = None,
        prebuffer_bytes: Optional[int] = None,
        total: Optional[int] = None,
        multiplex: Optional[Multiplexer] = None,
        strip_null_bytes_on_read: Optional[bool] = None,
        reverse: bool = False,
    ):
        # Resolving global defaults
        if prebuffer_bytes is None:
            prebuffer_bytes = DEFAULTS.prebuffer_bytes

        if strip_null_bytes_on_read is None:
            strip_null_bytes_on_read = DEFAULTS.strip_null_bytes_on_read

        if not isinstance(strip_null_bytes_on_read, bool):
            raise TypeError('expecting a boolean as "strip_null_bytes_on_read" kwarg')

        self.strip_null_bytes_on_read = strip_null_bytes_on_read

        if delimiter is None:
            data_format, inferred_delimiter = infer_delimiter_or_type(input_file)

            if data_format == "csv" and inferred_delimiter is not None:
                delimiter = inferred_delimiter

        # Detecting input type
        if isinstance(input_file, IOBase):
            input_type = "file"

        elif isinstance(input_file, (str, PathLike)):
            if isinstance(input_file, str) and looks_like_url(input_file):
                input_type = "url"
                input_file = request(input_file)
            else:
                input_type = "path"
                input_file = ensure_open(input_file, encoding=encoding)

        elif isinstance(input_file, Iterable):
            input_type = "iterable"
            input_file = iter(input_file)

        else:
            raise TypeError("expecting a file, a path or an iterable of rows")

        if multiplex is not None and not isinstance(multiplex, Multiplexer):
            raise TypeError("`multiplex` should be a casanova.Multiplexer")

        reader_kwargs = {}

        if dialect is not None:
            reader_kwargs["dialect"] = dialect
        if quotechar is not None:
            reader_kwargs["quotechar"] = quotechar
        if delimiter is not None:
            reader_kwargs["delimiter"] = delimiter

        self.input_type = input_type
        self.input_file = None
        self.backward_file = None

        if self.input_type == "iterable":
            if strip_null_bytes_on_read:
                self.reader = rows_without_null_bytes(input_file)
            else:
                self.reader = input_file

        else:
            self.input_file = input_file

            if self.input_type == "url":
                input_file = TextIOWrapper(
                    self.input_file, encoding=encoding, newline="", write_through=True
                )

            if strip_null_bytes_on_read:
                self.reader = csv.reader(
                    lines_without_null_bytes(input_file), **reader_kwargs
                )
            else:
                self.reader = ltpy311_csv_reader(input_file, **reader_kwargs)

        self.__buffered_rows = []
        self.was_completely_buffered = False
        self.total = total
        self.headers = None
        self.empty = False
        self.reverse = reverse
        self.no_headers = no_headers

        self.__no_headers_row_len = None

        # Reading headers
        if no_headers:
            try:
                self.__buffered_rows.append(next(self.reader))
            except StopIteration:
                self.empty = True
            else:
                self.__no_headers_row_len = len(self.__buffered_rows[0])
        else:
            try:
                fieldnames = next(self.reader)

                if fieldnames:
                    fieldnames[0] = suppress_BOM(fieldnames[0])

            except StopIteration:
                self.empty = True
            else:
                self.headers = Headers(fieldnames)

                try:
                    self.__buffered_rows.append(next(self.reader))
                except StopIteration:
                    self.empty = True

        # Reversing
        if reverse and not self.empty:
            if self.input_file is None or self.input_type == "url":
                raise NotImplementedError

            self.__buffered_rows.clear()
            (
                self.backward_file,
                backwards_lines_iterator,
            ) = create_csv_aware_backwards_lines_iterator(
                self.input_file,
                quotechar=quotechar,
                strip_null_bytes_on_read=strip_null_bytes_on_read,
            )

            self.reader = (
                ltpy311_csv_reader if not strip_null_bytes_on_read else csv.reader
            )(backwards_lines_iterator, **reader_kwargs)

            if not no_headers:
                self.reader = without_last(self.reader)

        # Multiplexing
        if multiplex is not None:
            if self.no_headers:
                multiplex_pos = multiplex.column

                if multiplex.new_column is not None:
                    raise TypeError(
                        "multiplexer cannot rename the column with no_headers=True"
                    )
            else:
                if multiplex.column not in self.headers:
                    raise MissingColumnError(multiplex.column)

                multiplex_pos = self.headers[multiplex.column]

            # New col
            if multiplex.new_column is not None:
                self.headers = Headers.rename(
                    self.headers, multiplex.column, multiplex.new_column
                )

            original_reader = self.reader
            already_buffered_rows = []

            if self.__buffered_rows:
                already_buffered_rows.append(self.__buffered_rows.pop())

            def multiplexing_reader():
                for row in chain(already_buffered_rows, original_reader):
                    cell = row[multiplex_pos]

                    if multiplex.separator not in cell:
                        yield row

                    else:
                        for value in cell.split(multiplex.separator):
                            copy = list(row)
                            copy[multiplex_pos] = value
                            yield copy

            self.reader = multiplexing_reader()

        # Prebuffering
        # NOTE: does not take into account probable first row of actual data
        # except the header one.
        if prebuffer_bytes is not None and self.total is None:
            if not isinstance(prebuffer_bytes, int) or prebuffer_bytes < 1:
                raise TypeError(
                    'expecting a positive integer as "prebuffer_bytes" kwarg'
                )

            buffered_bytes = 0

            while buffered_bytes < prebuffer_bytes:
                row = next(self.reader, None)

                if row is None:
                    self.was_completely_buffered = True
                    self.total = len(self.__buffered_rows)
                    break

                buffered_bytes += size_of_row_in_file(row)
                self.__buffered_rows.append(row)

        self.__buffered_rows.reverse()

        # Iteration state
        self.row_filter = None
        self.current_row_index = -1

        # Peeking state
        self.peeked_row = None
        self.peeked = False

    def __repr__(self) -> str:
        return "<%s>" % self.namespace

    @property
    def fieldnames(self) -> Optional[List[str]]:
        if self.headers is None:
            return None

        return self.headers.fieldnames

    @property
    def row_len(self) -> int:
        if self.__no_headers_row_len is not None:
            return self.__no_headers_row_len

        return len(self.headers)

    def peek(self) -> Optional[List[str]]:
        if self.peeked:
            return self.peeked_row

        try:
            self.peeked_row = next(self)
        except StopIteration:
            self.peeked_row = None

        self.peeked = True
        return self.peeked_row

    def __next__(self) -> List[str]:
        while True:
            if self.peeked:
                if self.peeked_row is None:
                    raise StopIteration

                row = self.peeked_row
                self.peeked = False
                return row

            if self.__buffered_rows:
                row = self.__buffered_rows.pop()
            else:
                row = next(self.reader)

            self.current_row_index += 1

            if self.row_filter is not None:
                if self.row_filter(self.current_row_index, row):
                    return row
            else:
                return row

    def rows(self) -> Iterator[List[str]]:
        while True:
            try:
                yield self.__next__()
            except StopIteration:
                return

    # NOTE: it is important for enricher inheritance to implement #.__iter__ as
    # relying on #.rows & not the other way around.
    def __iter__(self):
        return self.rows()

    # NOTE: this function exists because it takes into
    # account rows skipped by resumers, which is
    # essential for threadsafe operations etc.
    def enumerate(self, start=0):
        for row in self.rows():
            yield self.current_row_index + start, row

    def wrap(self, row):
        return self.headers.wrap(row)

    # NOTE: the underlying cell iterator is protected so
    # it remains easy to override it and reimplement it
    # when inheriting (check the threadsafe_enricher)
    # for such an example.
    def __cells(self, column, with_rows=False):
        if not isinstance(column, (str, int)):
            raise TypeError("target column should be given as a name or a position")

        if not isinstance(column, int):
            if self.headers is None:
                raise NoHeadersError

            pos = self.headers.get(column)

            if pos is None:
                raise MissingColumnError(column)
        else:
            if column >= self.row_len:
                raise MissingColumnError

            pos = column

        if with_rows:

            def iterator():
                for row in self.rows():
                    yield row, row[pos]

        else:

            def iterator():
                for row in self.rows():
                    yield row[pos]

        return iterator()

    def cells(self, column, *, with_rows=False):
        return self.__cells(column, with_rows=with_rows)

    def enumerate_cells(self, column, start=0, *, with_rows=False):
        if with_rows:
            for row, value in self.__cells(column, with_rows=True):
                yield self.current_row_index + start, row, value
        else:
            for value in self.__cells(column):
                yield self.current_row_index + start, value

    # NOTE: the underlying records iterator is protected so
    # it remains easy to override it and reimplement it
    # when inheriting (check the threadsafe_enricher)
    # for such an example.
    def __records(self, *shape, with_rows=False, ignore_headers=False):
        if len(shape) < 1:
            raise TypeError("shape of record was not given")

        if len(shape) == 1 and getattr(shape[0], "is_namedrecord", False):
            if (
                not ignore_headers
                and self.fieldnames is not None
                and self.fieldnames != shape[0].fieldnames
            ):
                raise TypeError("file headers are not aligned with the record ones")

            project = lambda r: shape[0](*r)

        elif len(shape) == 1 and is_tabular_record_class(shape[0]):
            if (
                not ignore_headers
                and self.fieldnames is not None
                and self.fieldnames != shape[0].fieldnames()
            ):
                raise TypeError("file headers are not aligned with the record ones")

            project = lambda r: shape[0].parse(r)
        elif self.no_headers:
            project = Headers.flat_project_no_headers(self.row_len, *shape)
        else:
            project = self.headers.flat_project(*shape)

        if with_rows:

            def iterator():
                for row in self.rows():
                    yield row, project(row)

        else:

            def iterator():
                for row in self.rows():
                    yield project(row)

        return iterator()

    def records(self, *shape, with_rows=False, ignore_headers=False):
        return self.__records(
            *shape, with_rows=with_rows, ignore_headers=ignore_headers
        )

    def enumerate_records(self, *shape, start=0, with_rows=False, ignore_headers=False):
        if with_rows:
            for row, record in self.__records(
                *shape, with_rows=True, ignore_headers=ignore_headers
            ):
                yield self.current_row_index + start, row, record
        else:
            for record in self.__records(*shape, ignore_headers=ignore_headers):
                yield self.current_row_index + start, record

    def close(self):
        if self.input_type == "url":
            self.input_file.release_conn()

        if self.input_file is not None and hasattr(self.input_file, "close"):
            self.input_file.close()

        if self.backward_file is not None:
            self.backward_file.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @classmethod
    def count(cls, input_file, max_rows=None, **kwargs) -> int:
        assert max_rows is None or max_rows > 0, (
            "%s.count: expected max_rows to be `None` or > 0." % cls.namespace
        )

        n = 0

        with cls(input_file, **kwargs) as reader:
            for _ in reader:
                n += 1

                if max_rows is not None and n > max_rows:
                    return None

        return n
