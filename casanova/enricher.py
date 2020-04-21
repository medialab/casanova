# =============================================================================
# Casanova Enricher
# =============================================================================
#
# A CSV reader/writer combo that can be used to read an input CSV file and
# easily ouput a similar CSV file while editing, adding and filtering cell_count.
#
import csv

from casanova.exceptions import NotResumableError
from casanova.reader import CasanovaReader, collect_column_indices
from casanova.utils import (
    is_resumable_buffer,
    is_empty_buffer
)


# TODO: we must go with events for resuming
# TODO: util to handle column and create pos
def make_enricher(name, namespace, Reader, immutable_rows=False):

    class AbstractCasanovaEnricher(Reader):
        def __init__(self, input_file, output_file, no_headers=False,
                     resumable=False, keep=None, add=None):

            # Inheritance
            super().__init__(
                input_file,
                no_headers=no_headers
            )

            # Sanity tests
            if resumable and not is_resumable_buffer(output_file):
                raise NotResumableError('%s: expecting an "a+" or "a+b" buffer.' % namespace)

            self.writer = csv.writer(output_file)
            self.keep_indices = None
            self.output_fieldnames = self.fieldnames
            self.added_count = 0
            self.padding = None
            self.immutable_rows = immutable_rows

            if keep is not None:
                self.keep_indices = collect_column_indices(self.pos, keep)
                self.output_fieldnames = self.filterrow(self.output_fieldnames)

            if add is not None:
                self.output_fieldnames += add
                self.added_count = len(add)
                self.padding = [''] * self.added_count

            # Need to write headers?
            if not no_headers:

                if not resumable or is_empty_buffer(output_file):
                    self.writeheader()

        def filterrow(self, row):
            if self.keep_indices is not None:
                row = [row[i] for i in self.keep_indices]
            elif self.immutable_rows:
                row = list(row)

            return row

        def writeheader(self):
            self.writer.writerow(self.output_fieldnames)

        def writerow(self, row):
            self.writer.writerow(row)

        def enrichrow(self, row, add=None):

            # Additions
            if self.added_count > 0:
                if add is None:
                    add = self.padding
                else:
                    assert len(add) == self.added_count, '%s.enrichrow: expected %i additional cells but got %i.' % (namespace, self.added_count, len(add))

                self.writer.writerow(self.filterrow(row) + add)

            # No additions
            else:
                assert add is None, '%s.enrichrow: expected no additions.' % namespace

                self.writer.writerow(self.fieldnames(row))

    return AbstractCasanovaEnricher


CasanovaEnricher = make_enricher(
    'CasanovaEnricher',
    'casanova.enricher',
    CasanovaReader
)
