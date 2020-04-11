# =============================================================================
# Casanova Enricher
# =============================================================================
#
# A CSV reader/writer combo that can be used to read an input CSV file and
# easily ouput a similar CSV file while editing, adding and filtering cells.
#
import csv
from casanova.reader import CasanovaReader


class CasanovaEnricher(CasanovaReader):
    def __init__(self, input_file, output_file, column=None, columns=None,
                 no_headers=False, resumable=False, keep=None, add=None):

        # Inheritance
        super().__init__(
            input_file,
            column=column,
            columns=columns,
            no_headers=no_headers
        )

        self.writer = csv.writer(output_file)

        # TODO: check output_file.mode

        # Writing headers
        fieldnames = self.headers

        if keep is not None:
            fieldnames = list(keep)

        if add is not None:
            fieldnames += list(add)

        self.fieldnames = fieldnames

        if not resumable:
            self.writer.writerow(fieldnames)


class ThreadSafeCasanovaEnricher(CasanovaEnricher):
    pass
