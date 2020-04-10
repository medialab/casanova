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
    def __init__(self, f, column=None, columns=None):
        assert column is not None or columns is not None, 'casanova.reader: expecting at least `column` or `columns`.'
        assert (column is None) != (columns is None), 'casanova.reader: expecting `column` or `columns` but not both.'

        # Target file
        self.f = f
        self.reader = csv.reader(f)

        try:
            self.headers = next(self.reader)
        except StopIteration:
            raise EmptyFileException

        if column is not None:
            self.single_pos = True
            self.column = column

            try:
                self.pos = self.headers.index(column)
            except ValueError:
                raise MissingHeaderException

    def __iter__(self):
        return self

    def __next__(self):
        line = next(self.reader)

        if self.single_pos:
            return line[self.pos]
