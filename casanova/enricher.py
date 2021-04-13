# =============================================================================
# Casanova Enricher
# =============================================================================
#
# A CSV reader/writer combo that can be used to read an input CSV file and
# easily ouput a similar CSV file while editing, adding and filtering cell_count.
#
import os
import csv

from casanova.resuming import Resumer
from casanova.contiguous_range_set import ContiguousRangeSet
from casanova.exceptions import (
    NotResumableError,
    ResumeError,
    MissingColumnError,
    CorruptedIndexColumn
)
from casanova.reader import (
    Reader,
    HeadersPositions
)


def make_enricher(name, namespace, Reader):

    class AbstractEnricher(Reader):
        __name__ = name

        def __init__(self, input_file, output_file, no_headers=False,
                     keep=None, add=None, prepend=None, dialect=None,
                     quotechar=None, delimiter=None):

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

            if prepend is not None:
                self.output_fieldnames = prepend + self.output_fieldnames

            self.output_pos = HeadersPositions(self.output_fieldnames if not no_headers else len(self.output_fieldnames))

            # Resuming?
            self.resumer = None
            can_resume = False

            if isinstance(output_file, Resumer):
                self.resumer = output_file

                can_resume = self.resumer.can_resume()

                if can_resume:
                    self.resumer.get_insights_from_output()

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

        def formatrow(self, row, add=None, index=None):

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

            if index is not None:
                row = [index] + row

            return row

        def writeheader(self):
            self.writer.writerow(self.output_fieldnames)

        def writerow(self, row, add=None):
            self.writer.writerow(self.formatrow(row, add))

    class AbstractThreadsafeEnricher(AbstractEnricher):
        __name__ = 'Threadsafe' + name

        def __init__(self, input_file, output_file, no_headers=False,
                     keep=None, add=None, index_column='index'):

            self.index_column = index_column
            self.already_done = ContiguousRangeSet()

            # Inheritance
            super().__init__(
                input_file,
                output_file,
                no_headers=no_headers,
                keep=keep,
                add=add,
                prepend=[index_column]
            )

        def __iter__(self):
            iterator = enumerate(super().__iter__())

            for index, row in iterator:
                if self.already_done.stateful_contains(index):
                    continue

                yield index, row

        # def resume(self):

        #     # Rolling back to beginning of file
        #     output_file = self.output_file

        #     if self.binary:
        #         output_file = open(output_file.name, 'rb')
        #     else:
        #         output_file.seek(0, os.SEEK_SET)

        #     reader = Reader(output_file, no_headers=self.fieldnames is None)

        #     should_emit = callable(self.listener)

        #     i = reader.pos.get(self.index_column)

        #     if i is None:
        #         raise MissingColumnError(self.index_column)

        #     for row in reader:
        #         try:
        #             current_index = int(row[i])
        #         except ValueError:
        #             raise CorruptedIndexColumn

        #         self.already_done.add(current_index)

        #         if should_emit:
        #             with self.event_lock:
        #                 self.listener('resume.output', row)

        #     self.already_done_count = len(self.already_done)

        #     if self.binary:
        #         output_file.close()

        def cells(self, column, with_rows=False):
            if with_rows:
                index = 0
                for row, value in super().cells(column, with_rows=True):
                    yield index, row, value
                    index += 1
            else:
                yield from enumerate(super().cells(column))

        def writerow(self, index, row, add=None):
            self.writer.writerow(self.formatrow(row, add, index=index))

    return AbstractThreadsafeEnricher, AbstractEnricher


ThreadsafeEnricher, Enricher = make_enricher(
    'Enricher',
    'casanova.enricher',
    Reader
)
