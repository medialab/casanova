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
    BatchResumer
)
from casanova.exceptions import MissingColumnError
from casanova.reader import (
    Reader,
    Headers
)


class Enricher(Reader):
    __supported_resumers__ = (RowCountResumer, LastCellComparisonResumer)

    def __init__(self, input_file, output_file, no_headers=False,
                 keep=None, add=None, **kwargs):

        # Inheritance
        super().__init__(input_file, no_headers=no_headers, **kwargs)

        self.keep_indices = None
        self.output_fieldnames = self.fieldnames
        self.added_count = 0
        self.padding = None

        if keep is not None:
            try:
                self.keep_indices = self.headers.collect(keep)
            except KeyError:
                raise MissingColumnError

            self.output_fieldnames = self.filterrow(self.output_fieldnames)

        if add is not None:
            self.output_fieldnames += add
            self.added_count = len(add)
            self.padding = [''] * self.added_count

        self.output_headers = None

        if self.headers is not None:
            self.output_headers = Headers(self.output_fieldnames if not no_headers else len(self.output_fieldnames))

        # Resuming?
        self.resumer = None
        can_resume = False

        if isinstance(output_file, Resumer):
            if not isinstance(output_file, self.__class__.__supported_resumers__):
                raise TypeError('%s: does not support %s!' % (self.__class__.__name__, output_file.__class__.__name__))

            self.resumer = output_file

            can_resume = self.resumer.can_resume()

            if can_resume:
                self.resumer.get_insights_from_output(self)

                if hasattr(self.resumer, 'resume'):
                    self.resumer.resume(self)

            output_file = self.resumer.open_output_file()

        # Instantiating writer
        self.writer = csv.writer(output_file)

        # Need to write headers?
        if not no_headers and not can_resume:
            self.writeheader()

    # NOTE: overriding #.iter and not #.__iter__ else other reader iterators won't work
    def iter(self):
        if self.resumer is None:
            yield from super().iter()
            return

        if not hasattr(self.resumer, 'filter'):
            yield from self.resumer
            yield from super().iter()
            return

        iterator = enumerate(super().iter())

        for i, row in iterator:
            if self.resumer.filter_row(i, row):
                yield row

    def __repr__(self):
        columns_info = ' '.join('%s=%s' % t for t in self.headers)

        return '<%s%s %s>' % (
            self.__class__.__name__,
            ' resumable' if self.resumable else '',
            columns_info
        )

    def filterrow(self, row):
        if self.keep_indices is not None:
            row = [row[i] for i in self.keep_indices]

        return row

    def formatrow(self, row, add=None):

        # Additions
        if self.added_count > 0:
            if add is None:
                add = self.padding
            else:
                assert len(add) == self.added_count, 'casanova.enricher.writerow: expected %i additional cells but got %i.' % (self.added_count, len(add))

            row = self.filterrow(row) + add

        # No additions
        else:
            assert add is None, 'casanova.enricher.writerow: expected no additions.'

            row = self.filterrow(row)

        return row

    def writeheader(self):
        self.writer.writerow(self.output_fieldnames)

    def writerow(self, row, add=None):
        self.writer.writerow(self.formatrow(row, add))


class ThreadSafeEnricher(Enricher):
    __supported_resumers__ = (ThreadSafeResumer,)

    def __init__(self, input_file, output_file, add=None,
                    index_column='index', **kwargs):

        self.index_column = index_column

        # Inheritance
        super().__init__(
            input_file,
            output_file,
            add=[index_column] + list(add),
            **kwargs
        )

    def __iter__(self):
        yield from enumerate(super().__iter__())

    def cells(self, column, with_rows=False):
        if with_rows:
            for index, (row, value) in enumerate(super().cells(column, with_rows=True)):
                yield index, row, value
        else:
            yield from enumerate(super().cells(column))

    def writerow(self, index, row, add=None):
        index = [index]

        if add is None:
            add = self.padding

        self.writer.writerow(self.formatrow(row, index + add))


class BatchEnricher(Enricher):
    __supported_resumers__ = (BatchResumer,)

    def __init__(self, input_file, output_file, add=None, cursor_column='cursor',
                    end_symbol='end', **kwargs):

        self.cursor_column = cursor_column
        self.end_symbol = end_symbol

        # Inheritance
        super().__init__(
            input_file,
            output_file,
            add=[cursor_column] + list(add),
            **kwargs
        )

    def writebatch(self, row, batch, cursor=None):
        if cursor is None:
            cursor = self.end_symbol

        for is_last, addendum in with_is_last(batch):
            self.writerow(row, [cursor if is_last else None] + addendum)
