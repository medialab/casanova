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

from casanova.exceptions import NotResumableError, ResumeError
from casanova.reader import CasanovaReader, collect_column_indices
from casanova.utils import (
    is_resumable_buffer,
    is_empty_buffer
)


def make_enricher(name, namespace, Reader, immutable_rows=False):

    class AbstractCasanovaEnricher(Reader):
        def __init__(self, input_file, output_file, no_headers=False,
                     resumable=False, keep=None, add=None, listener=None,
                     unordered=False, index_column='index'):

            # Inheritance
            super().__init__(
                input_file,
                no_headers=no_headers
            )

            # Sanity tests
            if resumable and not is_resumable_buffer(output_file):
                raise NotResumableError('%s: expecting an "a+" or "a+b" buffer.' % namespace)

            self.output_file = output_file
            self.writer = csv.writer(output_file)
            self.keep_indices = None
            self.output_fieldnames = self.fieldnames
            self.added_count = 0
            self.padding = None

            self.immutable_rows = immutable_rows

            self.unordered = unordered
            self.resumable = resumable
            self.resume_offset = 0

            self.listener = listener
            self.event_lock = Lock()

            if keep is not None:
                self.keep_indices = collect_column_indices(self.pos, keep)
                self.output_fieldnames = self.filterrow(self.output_fieldnames)

            if add is not None:
                self.output_fieldnames += add
                self.added_count = len(add)
                self.padding = [''] * self.added_count

            if unordered:
                self.output_fieldnames = [index_column] + self.output_fieldnames

            # Need to write headers?
            output_buffer_is_empty = is_empty_buffer(output_file)

            if not no_headers:

                if not resumable or output_buffer_is_empty:
                    self.writeheader()

            # Resuming
            if resumable and not output_buffer_is_empty:
                self.resume()

        def __repr__(self):
            columns_info = ' '.join('%s=%s' % t for t in zip(self.pos._fields, self.pos))

            return '<%s%s%s %s>' % (
                namespace,
                ' resumable' if self.resumable else '',
                ' unordered' if self.unordered else '',
                columns_info
            )

        def resume(self):

            # Rolling back to beginning of file
            output_file = self.output_file

            if self.binary:
                output_file = open(output_file.name, 'rb')
            else:
                output_file.seek(0, os.SEEK_SET)

            reader = Reader(output_file, no_headers=self.fieldnames is None)

            should_emit = callable(self.listener)

            for row in reader:
                self.resume_offset += 1

                if should_emit:
                    self.listener('resume.output', row)

            if self.binary:
                output_file.close()

            i = 0

            while i < self.resume_offset:
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
            elif self.immutable_rows:
                row = list(row)

            return row

        def formatrow(self, row, add=None):

            # Additions
            if self.added_count > 0:
                if add is None:
                    add = self.padding
                else:
                    assert len(add) == self.added_count, '%s.enrichrow: expected %i additional cells but got %i.' % (namespace, self.added_count, len(add))

                return self.filterrow(row) + add

            # No additions
            else:
                assert add is None, '%s.enrichrow: expected no additions.' % namespace

                return self.filterrow(row)

        def writeheader(self):
            self.writer.writerow(self.output_fieldnames)

        def writerow(self, row):
            self.writer.writerow(row)

        def enrichrow(self, row, add=None):
            self.writer.writerow(self.formatrow(row, add))

    return AbstractCasanovaEnricher


CasanovaEnricher = make_enricher(
    'CasanovaEnricher',
    'casanova.enricher',
    CasanovaReader
)
