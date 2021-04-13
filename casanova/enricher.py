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
    Resumer,
    LineCountResumer,
    ThreadSafeResumer,
    BatchResumer
)
from casanova.exceptions import MissingColumnError
from casanova.reader import (
    Reader,
    HeadersPositions
)


def make_enricher(name, namespace, Reader):

    class AbstractEnricher(Reader):
        __supported_resumers__ = (LineCountResumer,)

        def __init__(self, input_file, output_file, no_headers=False,
                     keep=None, add=None, dialect=None, quotechar=None,
                     delimiter=None):

            # Inheritance
            reader_kwargs = {
                'no_headers': no_headers
            }

            if 'monkey' not in namespace:
                reader_kwargs['dialect'] = dialect
                reader_kwargs['quotechar'] = quotechar
                reader_kwargs['delimiter'] = delimiter

            super().__init__(input_file, **reader_kwargs)

            self.keep_indices = None
            self.output_fieldnames = self.fieldnames
            self.added_count = 0
            self.padding = None

            if keep is not None:
                try:
                    self.keep_indices = self.pos.collect(keep)
                except KeyError:
                    raise MissingColumnError

                self.output_fieldnames = self.filterrow(self.output_fieldnames)

            if add is not None:
                self.output_fieldnames += add
                self.added_count = len(add)
                self.padding = [''] * self.added_count

            self.output_pos = HeadersPositions(self.output_fieldnames if not no_headers else len(self.output_fieldnames))

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

                output_file = self.resumer.open_output_file()

            # Instantiating writer
            self.writer = csv.writer(output_file)

            # Need to write headers?
            if not no_headers and not can_resume:
                self.writeheader()

        def __iter__(self):
            if self.resumer is None:
                yield from super().__iter__()

            iterator = enumerate(super().__iter__())

            for i, row in iterator:
                if self.resumer.filter_already_done_row(i, row):
                    yield row

        def __repr__(self):
            columns_info = ' '.join('%s=%s' % t for t in self.pos)

            return '<%s%s %s>' % (
                namespace,
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
                    assert len(add) == self.added_count, '%s.writerow: expected %i additional cells but got %i.' % (namespace, self.added_count, len(add))

                row = self.filterrow(row) + add

            # No additions
            else:
                assert add is None, '%s.writerow: expected no additions.' % namespace

                row = self.filterrow(row)

            return row

        def writeheader(self):
            self.writer.writerow(self.output_fieldnames)

        def writerow(self, row, add=None):
            self.writer.writerow(self.formatrow(row, add))

    class AbstractThreadSafeEnricher(AbstractEnricher):
        __supported_resumers__ = (ThreadSafeResumer,)

        def __init__(self, input_file, output_file, no_headers=False,
                     keep=None, add=None, index_column='index'):

            self.index_column = index_column

            # Inheritance
            super().__init__(
                input_file,
                output_file,
                no_headers=no_headers,
                keep=keep,
                add=[index_column] + list(add)
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

    class AbstractBatchEnricher(AbstractEnricher):
        __supported_resumers__ = (BatchResumer,)

        def __init__(self, input_file, output_file, no_headers=False,
                     keep=None, add=None, cursor_column='cursor', end_symbol='end'):

            self.cursor_column = cursor_column
            self.end_symbol = end_symbol

            # Inheritance
            super().__init__(
                input_file,
                output_file,
                no_headers=no_headers,
                keep=keep,
                add=[cursor_column] + list(add)
            )

        def writebatch(self, row, batch, cursor=None):
            if cursor is None:
                cursor = self.end_symbol

            for is_last, addendum in with_is_last(batch):
                self.writerow(row, [cursor if is_last else None] + addendum)

    AbstractEnricher.__name__ = name
    AbstractThreadSafeEnricher.__name__ = 'ThreadSafe' + name
    AbstractBatchEnricher.__name__ = 'Batch' + name

    return (
        AbstractEnricher,
        AbstractThreadSafeEnricher,
        AbstractBatchEnricher
    )


Enricher, ThreadSafeEnricher, BatchEnricher = make_enricher(
    'Enricher',
    'casanova.enricher',
    Reader
)
