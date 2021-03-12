# =============================================================================
# Casanova Enricher
# =============================================================================
#
# A CSV reader/writer combo that can be used to read an input CSV file and
# easily ouput a similar CSV file while editing, adding and filtering cell_count.
#
import os
import csv
from threading import Lock

from casanova.contiguous_range_set import ContiguousRangeSet
from casanova.exceptions import (
    NotResumableError,
    ResumeError,
    MissingColumnError,
    CorruptedIndexColumn
)
from casanova.reader import (
    CasanovaReader,
    HeadersPositions,
    get_column_index,
    collect_column_indices
)
from casanova.utils import (
    is_resumable_buffer,
    is_empty_buffer,
    is_mute_buffer
)


def make_enricher(name, namespace, Reader):

    class AbstractCasanovaEnricher(Reader):
        __name__ = name

        def __init__(self, input_file, output_file, no_headers=False,
                     resumable=False, auto_resume=True, keep=None, add=None,
                     listener=None, prepend=None, dialect=None, quotechar=None,
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

            # Sanity tests
            if resumable and not is_resumable_buffer(output_file):
                raise NotResumableError('%s: expecting an "a+" or "a+b" buffer.' % namespace)

            self.output_file = output_file
            self.writer = csv.writer(output_file)
            self.keep_indices = None
            self.output_fieldnames = self.fieldnames
            self.added_count = 0
            self.padding = None

            self.resumable = resumable
            self.should_resume = False
            self.already_done_count = 0

            self.listener = listener

            if keep is not None:
                self.keep_indices = collect_column_indices(self.pos, keep)
                self.output_fieldnames = self.filterrow(self.output_fieldnames)

            if add is not None:
                self.output_fieldnames += add
                self.added_count = len(add)
                self.padding = [''] * self.added_count

            if prepend is not None:
                self.output_fieldnames = prepend + self.output_fieldnames

            self.output_pos = HeadersPositions(self.output_fieldnames if not no_headers else len(self.output_fieldnames))

            # Need to write headers?
            output_buffer_is_empty = is_mute_buffer(output_file) or is_empty_buffer(output_file)

            if not no_headers:

                if not resumable or output_buffer_is_empty:
                    self.writeheader()

            # Resuming
            if resumable and not output_buffer_is_empty:
                self.should_resume = True

                if auto_resume:
                    self.resume()

        def __repr__(self):
            columns_info = ' '.join('%s=%s' % t for t in zip(self.pos._fields, self.pos))

            return '<%s%s%s %s>' % (
                namespace,
                ' resumable' if self.resumable else '',
                ' unordered' if getattr(self, 'unordered', False) else '',
                columns_info
            )

        def resume(self):

            if not self.should_resume:
                return

            self.should_resume = False

            # Rolling back to beginning of file
            output_file = self.output_file

            if self.binary:
                output_file = open(output_file.name, 'rb')
            else:
                output_file.seek(0, os.SEEK_SET)

            reader = Reader(output_file, no_headers=self.fieldnames is None)

            should_emit = callable(self.listener)

            if should_emit:
                self.listener('resume.start', None)

            for row in reader:
                self.already_done_count += 1

                if should_emit:
                    self.listener('resume.output', row)

            if self.binary:
                output_file.close()

            i = 0

            while i < self.already_done_count:
                try:
                    row = next(self.reader)

                    if should_emit:
                        self.listener('resume.input', row)

                    i += 1
                except StopIteration:
                    raise ResumeError('%s.resume: output has more lines than input.' % namespace)

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

    class AbstractThreadsafeCasanovaEnricher(AbstractCasanovaEnricher):
        __name__ = 'Threadsafe' + name

        def __init__(self, input_file, output_file, no_headers=False,
                     resumable=False, auto_resume=True, keep=None, add=None,
                     listener=None, index_column='index'):

            self.index_column = index_column
            self.event_lock = Lock()
            self.already_done = ContiguousRangeSet()

            # Inheritance
            super().__init__(
                input_file,
                output_file,
                no_headers=no_headers,
                resumable=resumable,
                keep=keep,
                add=add,
                listener=listener,
                prepend=[index_column]
            )

        def __iter__(self):
            iterator = enumerate(super().__iter__())
            should_emit = callable(self.listener)

            for index, row in iterator:
                if self.already_done.stateful_contains(index):
                    if should_emit:
                        with self.event_lock:
                            self.listener('resume.input', row)

                    continue

                yield index, row

        def resume(self):

            # Rolling back to beginning of file
            output_file = self.output_file

            if self.binary:
                output_file = open(output_file.name, 'rb')
            else:
                output_file.seek(0, os.SEEK_SET)

            reader = Reader(output_file, no_headers=self.fieldnames is None)

            should_emit = callable(self.listener)

            i = get_column_index(reader.pos, self.index_column)

            if i is None:
                raise MissingColumnError(self.index_column)

            for row in reader:
                try:
                    current_index = int(row[i])
                except ValueError:
                    raise CorruptedIndexColumn

                self.already_done.add(current_index)

                if should_emit:
                    with self.event_lock:
                        self.listener('resume.output', row)

            self.already_done_count = len(self.already_done)

            if self.binary:
                output_file.close()

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

    return AbstractThreadsafeCasanovaEnricher, AbstractCasanovaEnricher


ThreadsafeCasanovaEnricher, CasanovaEnricher = make_enricher(
    'CasanovaEnricher',
    'casanova.enricher',
    CasanovaReader
)
