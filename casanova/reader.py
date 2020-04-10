# =============================================================================
# Casanova Reader
# =============================================================================
#
# A fast but comfortable CSV reader based upon csv.reader to avoid dealing
# with csv.DictReader which is nice but very slow.
#
import csv
from casanova.exceptions import EmptyFileException, MissingHeaderException


class CasanovaReader(object):
    def __init__(self, f, column=None, columns=None, no_headers=False):
        if column is None and columns is None:
            raise TypeError('casanova.reader: expecting at least `column` or `columns`!')

        if column is not None and columns is not None:
            raise TypeError('casanova.reader: expecting `column` or `columns` but not both!')

        # Target file
        self.f = f
        self.reader = csv.reader(f)

        if not no_headers:
            try:
                self.headers = next(self.reader)
            except StopIteration:
                raise EmptyFileException
        else:
            self.headers = None

        if column is not None:
            self.single_pos = True
            self.column = column

            if no_headers and not isinstance(column, int):
                raise TypeError('casanova.reader: `column` should be an int if `no_headers` is True!')

            try:
                if isinstance(column, int):
                    self.pos = column
                else:
                    self.pos = self.headers.index(column)
            except ValueError:
                raise MissingHeaderException

    def __iter__(self):
        return self

    def __next__(self):
        line = next(self.reader)

        if self.single_pos:
            return line[self.pos]
