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


class CasanovaRecord(object):
    def __init__(self, columns, pos):
        self.columns = columns
        self.pos = pos
        self.length = len(pos)
        self.row = None

        self.attr_to_pos = {}

        for i, column in enumerate(self.columns):
            self.attr_to_pos[column] = self.pos[i]

    def set_row(self, row):
        self.row = row

    def __len__(self):
        return self.length

    def __getitem__(self, index):
        if index >= self.length:
            raise IndexError

        return self.row[index]

    def __getattr__(self, attr):
        index = self.attr_to_pos.get(attr)

        if index is None:
            raise AttributeError

        return self.row[index]

    def __iter__(self):
        for pos in self.pos:
            yield self.row[pos]

    def __repr__(self):
        parts = []

        for i, column in enumerate(self.columns):
            parts.append('%s=%s' % (column, self.row[self.pos[i]]))

        return 'CasanovaRecord(' + ', '.join(parts) + ')'


class CasanovaReader(object):
    def __init__(self, f, column=None, columns=None, no_headers=False):
        if column is None and columns is None:
            raise TypeError('casanova.reader: expecting at least `column` or `columns`!')

        if column is not None and columns is not None:
            raise TypeError('casanova.reader: expecting `column` or `columns` but not both!')

        # Target file
        self.f = f
        self.reader = csv.reader(f)
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

            t = namedtuple('CasanovaReaderPositions', columns)
            self.pos = t(*self.pos)
            self.record = CasanovaRecord(columns, self.pos)

    def __iter__(self):
        return self

    def __next__(self):
        row = next(self.reader)

        if self.single_pos:
            return row[self.pos]
        else:
            self.record.set_row(row)
            return self.record

    def rows(self):
        return iter(self.reader)
