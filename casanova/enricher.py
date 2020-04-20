# =============================================================================
# Casanova Enricher
# =============================================================================
#
# A CSV reader/writer combo that can be used to read an input CSV file and
# easily ouput a similar CSV file while editing, adding and filtering cell_count.
#
import csv
from collections import namedtuple

from casanova.reader import CasanovaReader
from casanova.exceptions import MissingColumnError, ColumnNumberMismatchError


# TODO: we must go with events for resuming
# TODO: util to handle column and create pos
class CasanovaEnricher(CasanovaReader):
    def __init__(self, input_file, output_file, no_headers=False,
                 resumable=False, keep=None, add=None):

        # Inheritance
        super().__init__(
            input_file,
            no_headers=no_headers
        )

        self.writer = csv.writer(output_file)

        self.keep = keep
        self.keep_indices = None

        # if keep is not None:
        #     for column in keep:
        #         i = self.pos.get()


# TODO: lock for events
class ThreadSafeCasanovaEnricher(CasanovaEnricher):
    pass
