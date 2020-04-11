# =============================================================================
# Casanova Enricher
# =============================================================================
#
# A CSV reader/writer combo that can be used to read an input CSV file and
# easily ouput a similar CSV file while editing, adding and filtering cells.
#
import csv
from collections import namedtuple

from casanova.reader import CasanovaReader
from casanova.exceptions import MissingHeaderException, ColumnNumberMismatch


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
        self.keep = None
        self.keep_pos = None
        self.keep_record = None

        # Columns to keep
        if keep is not None:
            self.keep = list(keep)
            fieldnames = self.keep

            self.keep_pos = None

            for column in self.keep:
                try:
                    if isinstance(column, int):
                        self.keep_pos.append(column)
                    else:
                        self.keep_pos.append(self.headers.index(column))
                except ValueError:
                    raise MissingHeaderException(column)

            self.keep_record = namedtuple('CasanovaRecord', self.keep)
            self.keep_pos = self.keep_record(*self.keep_pos)

        # Columns to add
        if add is not None:
            fieldnames += list(add)

        self.fieldnames = fieldnames
        self.cells = len(fieldnames)
        self.padding = [''] * self.cells

        if not resumable:
            self.writer.writerow(fieldnames)

    def __filter_row(self, row):
        if self.keep is not None:
            return [row[p] for p in self.keep_pos]

        return row

    def writerow(self, row):
        self.writer.writerow(row)

    def enrichrow(self, additions=None):
        if len(additions) != self.cells:
            raise ColumnNumberMismatch

        if additions is None:
            additions = self.padding

        self.writerow(self.__filter_row(self.current_row) + additions)


class ThreadSafeCasanovaEnricher(CasanovaEnricher):
    def enrichrow(self, row, additions=None):
        if len(additions) != self.cells:
            raise ColumnNumberMismatch

        if additions is None:
            additions = self.padding

        self.writerow(self.__filter_row(row) + additions)
