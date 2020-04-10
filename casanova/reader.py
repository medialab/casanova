# =============================================================================
# Casanova Reader
# =============================================================================
#
# A fast but comfortable CSV reader based upon csv.reader to avoid dealing
# with csv.DictReader which is nice but very slow.
#
import csv
from collections import namedtuple

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
        self.current_row = None
        self.record = None

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
                raise MissingHeaderException(column)

        if columns is not None:
            columns = list(columns)

            self.single_pos = False
            self.columns = columns
            self.pos = []

            for column in columns:
                if no_headers and not isinstance(column, int):
                    raise TypeError('casanova.reader: `columns` should only contain ints if `no_headers` is True!')

                try:
                    if isinstance(column, int):
                        self.pos.append(column)
                    else:
                        self.pos.append(self.headers.index(column))
                except ValueError:
                    raise MissingHeaderException(column)

            self.record = namedtuple('CasanovaRecord', columns)
            self.pos = self.record(*self.pos)

    def __iter__(self):
        return self

    def __next__(self):
        row = next(self.reader)
        self.current_row = row

        if self.single_pos:
            return row[self.pos]
        else:
            record = self.record(*(row[pos] for pos in self.pos))
            return record

    def rows(self):
        return iter(self.reader)
