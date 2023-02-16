# =============================================================================
# Casanova Enricher
# =============================================================================
#
# A CSV reader/writer combo that can be used to read an input CSV file and
# easily ouput a similar CSV file while editing, adding and filtering cell_count.
#
import csv
from ebbe import with_is_last

from casanova.resuming import (
    LastCellComparisonResumer,
    Resumer,
    RowCountResumer,
    ThreadSafeResumer,
    BatchResumer,
)
from casanova.exceptions import MissingColumnError
from casanova.reader import Reader, Headers
from casanova.defaults import DEFAULTS
from casanova.utils import strip_null_bytes_from_row, py310_wrap_csv_writerow


class Enricher(Reader):
    __supported_resumers__ = (RowCountResumer, LastCellComparisonResumer)

    def __init__(
        self,
        input_file,
        output_file,
        no_headers=False,
        select=None,
        add=None,
        strip_null_bytes_on_write=None,
        writer_dialect=None,
        writer_delimiter=None,
        writer_quotechar=None,
        writer_escapechar=None,
        writer_quoting=None,
        writer_lineterminator=None,
        **kwargs
    ):
        # Inheritance
        super().__init__(input_file, no_headers=no_headers, **kwargs)

        if strip_null_bytes_on_write is None:
            strip_null_bytes_on_write = DEFAULTS["strip_null_bytes_on_write"]

        if not isinstance(strip_null_bytes_on_write, bool):
            raise TypeError('expecting a boolean as "strip_null_bytes_on_write" kwarg')

        self.strip_null_bytes_on_write = strip_null_bytes_on_write

        self.selected_indices = None
        self.output_fieldnames = self.fieldnames
        self.added_count = 0
        self.padding = None

        if select is not None:
            try:
                self.selected_indices = self.headers.select(select)
            except KeyError:
                raise MissingColumnError

            self.output_fieldnames = self.filterrow(self.output_fieldnames)

        if add is not None:
            self.output_fieldnames += add
            self.added_count = len(add)
            self.padding = [""] * self.added_count

        self.output_headers = None

        if self.headers is not None:
            self.output_headers = Headers(
                self.output_fieldnames
                if not no_headers
                else len(self.output_fieldnames)
            )

        # Resuming?
        self.resumer = None
        can_resume = False

        if isinstance(output_file, Resumer):
            if not isinstance(output_file, self.__class__.__supported_resumers__):
                raise TypeError(
                    "%s: does not support %s!"
                    % (self.__class__.__name__, output_file.__class__.__name__)
                )

            self.resumer = output_file

            can_resume = self.resumer.can_resume()

            if can_resume:
                self.resumer.get_insights_from_output(self)

                if hasattr(self.resumer, "resume"):
                    self.resumer.resume(self)

            output_file = self.resumer.open_output_file()

            if hasattr(self.resumer, "filter"):
                self.row_filter = self.resumer.filter_row
            else:
                self.prelude_rows = self.resumer

        # Instantiating writer
        writer_kwargs = {}

        if writer_dialect is not None:
            writer_kwargs["dialect"] = writer_dialect

        if writer_delimiter is not None:
            writer_kwargs["delimiter"] = writer_delimiter

        if writer_quotechar is not None:
            writer_kwargs["quotechar"] = writer_quotechar

        if writer_escapechar is not None:
            writer_kwargs["escapechar"] = writer_escapechar

        if writer_quoting is not None:
            writer_kwargs["quoting"] = writer_quoting

        if writer_lineterminator is not None:
            writer_kwargs["lineterminator"] = writer_lineterminator

        self.writer = csv.writer(output_file, **writer_kwargs)
        self._writerow = self.writer.writerow

        if not strip_null_bytes_on_write:
            self._writerow = py310_wrap_csv_writerow(self.writer)

        # Need to write headers?
        if self.output_fieldnames and not no_headers and not can_resume:
            self.__writeheader()

    def __repr__(self):
        return "<%s>" % self.__class__.__name__

    def filterrow(self, row):
        if self.selected_indices is not None:
            row = [row[i] for i in self.selected_indices]

        return row

    def formatrow(self, row, add=None):
        # Additions
        if self.added_count > 0:
            if add is None:
                add = self.padding
            else:
                if not isinstance(add, list):
                    add = list(add)

                assert len(add) == self.added_count, (
                    "casanova.enricher.writerow: expected %i additional cells but got %i."
                    % (self.added_count, len(add))
                )

            row = self.filterrow(row) + add

        # No additions
        else:
            assert add is None, "casanova.enricher.writerow: expected no additions."

            row = self.filterrow(row)

        # NOTE: maybe it could be faster to wrap writerow altogether
        # instead of running this condition on each write
        if self.strip_null_bytes_on_write:
            row = strip_null_bytes_from_row(row)

        return row

    def __writeheader(self):
        self._writerow(self.output_fieldnames)

    def writerow(self, row, add=None):
        self._writerow(self.formatrow(row, add))


class ThreadSafeEnricher(Enricher):
    __supported_resumers__ = (ThreadSafeResumer,)

    def __init__(
        self, input_file, output_file, add=None, index_column="index", **kwargs
    ):
        self.index_column = index_column

        # Inheritance
        super().__init__(
            input_file, output_file, add=[index_column] + list(add or []), **kwargs
        )

    def __iter__(self):
        return self.enumerate()

    def cells(self, column, with_rows=False):
        if with_rows:
            for row, value in super().cells(column, with_rows=True):
                yield self.current_row_index, row, value
        else:
            for value in super().cells(column):
                yield self.current_row_index, value

    def writerow(self, index, row, add=None):
        super().writerow(row, add=[index] + (add or []))


class BatchEnricher(Enricher):
    __supported_resumers__ = (BatchResumer,)

    def __init__(
        self,
        input_file,
        output_file,
        add=None,
        cursor_column="cursor",
        end_symbol="end",
        **kwargs
    ):
        self.cursor_column = cursor_column
        self.end_symbol = end_symbol

        # Inheritance
        super().__init__(
            input_file, output_file, add=[cursor_column] + list(add), **kwargs
        )

    def writebatch(self, row, batch, cursor=None):
        if cursor is None:
            cursor = self.end_symbol

        for is_last, addendum in with_is_last(batch):
            self.writerow(row, [cursor if is_last else None] + addendum)
