# =============================================================================
# Casanova Enricher
# =============================================================================
#
# A CSV reader/writer combo that can be used to read an input CSV file and
# easily ouput a similar CSV file while editing, adding and filtering cell_count.
#
from ebbe import with_is_last

from casanova.resumers import (
    LastCellComparisonResumer,
    Resumer,
    RowCountResumer,
    ThreadSafeResumer,
    BatchResumer,
)
from casanova.headers import Headers
from casanova.reader import Reader
from casanova.writer import Writer
from casanova.defaults import DEFAULTS


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
        write_header=True,
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
            if no_headers:
                self.selected_indices = Headers.select_no_headers(self.row_len, select)
                print(self.selected_indices)
            else:
                self.selected_indices = self.headers.select(select)
                self.output_fieldnames = self.filterrow(self.output_fieldnames)

        if add is not None:
            if no_headers:
                if not isinstance(add, int):
                    raise TypeError(
                        "cannot add named columns with no_headers=True. Use a number of columns instead."
                    )

                self.added_count = add
            else:
                self.output_fieldnames += add
                self.added_count = len(add)

            self.padding = [""] * self.added_count

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
                # NOTE: how about null bytes
                self.resumer.get_insights_from_output(
                    self,
                    no_headers=no_headers,
                    dialect=writer_dialect,
                    quotechar=writer_quotechar,
                    delimiter=writer_delimiter,
                )

                if hasattr(self.resumer, "resume"):
                    self.resumer.resume(self)

            output_file = self.resumer.open_output_file()

            if hasattr(self.resumer, "filter"):
                self.row_filter = self.resumer.filter_row
            else:
                self.prelude_rows = self.resumer

        # Instantiating writer
        self.writer = Writer(
            output_file,
            fieldnames=self.output_fieldnames,
            strip_null_bytes_on_write=strip_null_bytes_on_write,
            dialect=writer_dialect,
            delimiter=writer_delimiter,
            quotechar=writer_quotechar,
            escapechar=writer_escapechar,
            quoting=writer_quoting,
            lineterminator=writer_lineterminator,
            write_header=not can_resume and write_header,
        )

    def __repr__(self):
        return "<%s>" % self.__class__.__name__

    @property
    def output_headers(self):
        return self.writer.headers

    @property
    def should_write_header(self):
        return self.writer.should_write_header

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

                if len(add) != self.added_count:
                    raise TypeError(
                        "casanova.enricher.writerow: expected %i additional cells but got %i."
                        % (self.added_count, len(add))
                    )

            row = self.filterrow(row) + add

        # No additions
        else:
            if add:
                raise TypeError("casanova.enricher.writerow: expected no additions.")

            row = self.filterrow(row)

        return row

    def writeheader(self):
        self.writer.writeheader()

    def writerow(self, row, add=None):
        self.writer.writerow(self.formatrow(row, add))


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
