# =============================================================================
# Casanova Reverse Reader
# =============================================================================
#
# A reader reading the file backwards in order to read last lines in constant
# time. This is sometimes useful to be able to resume some computations
# where they were left off.
#
import csv
from io import DEFAULT_BUFFER_SIZE
from file_read_backwards.file_read_backwards import FileReadBackwardsIterator
from ebbe import with_is_last

from casanova.reader import CasanovaReader
from casanova.utils import ensure_open
from casanova.exceptions import EmptyFileError

END_OF_FILE = object()


class Batch(object):
    __slots__ = ('value', 'finished', 'cursor', 'rows')

    def __init__(self, value, finished=False, cursor=None, rows=None):
        self.value = value
        self.finished = finished
        self.cursor = cursor
        self.rows = rows or []

    def __eq__(self, other):
        return (
            self.value == other.value and
            self.finished == other.finished and
            self.cursor == other.cursor and
            self.rows == other.rows
        )

    def __iter__(self):
        return iter(self.rows)

    def collect(self, pos):
        return set(row[pos] for row in self)

    def __repr__(self):
        class_name = self.__class__.__name__

        return (
            '<%(class_name)s value=%(value)s finished=%(finished)s cursor=%(cursor)s rows=%(rows)i>'
        ) % {
            'class_name': class_name,
            'value': self.value,
            'finished': self.finished,
            'cursor': self.cursor,
            'rows': len(self.rows)
        }


class CasanovaReverseReader(CasanovaReader):
    namespace = 'casanova.reverse_reader'

    def __init__(self, input_file, **kwargs):
        super().__init__(input_file, **kwargs)

        self.backwards_file = ensure_open(self.input_file.name, mode='rb')

        backwards_iterator = FileReadBackwardsIterator(
            self.backwards_file,
            self.input_file.encoding,
            DEFAULT_BUFFER_SIZE
        )

        backwards_reader = csv.reader(backwards_iterator)

        def generator():

            for is_last, row in with_is_last(backwards_reader):
                if not is_last or self.fieldnames is None:
                    yield row

            self.close()

        self.reader = generator()

        if self.fieldnames is None:
            self.buffered_rows = []

    def close(self):
        super().close()
        self.backwards_file.close()

    @staticmethod
    def last_cell(input_file, column, **kwargs):
        with CasanovaReverseReader(input_file, **kwargs) as reader:
            record = next(reader.cells(column), END_OF_FILE)

            if record is END_OF_FILE:
                raise EmptyFileError

            return record

    @staticmethod
    def last_batch(input_file, batch_value, batch_cursor, end_symbol, **kwargs):
        with CasanovaReverseReader(input_file, **kwargs) as reader:
            batch = END_OF_FILE

            for row, (value, cursor) in reader.cells((batch_value, batch_cursor), with_rows=True):
                if batch is END_OF_FILE:
                    batch = Batch(value)

                if value != batch.value:
                    return batch

                if cursor == end_symbol:
                    batch.finished = True
                    return batch

                if cursor:
                    batch.cursor = cursor
                    return batch

                batch.rows.append(row)

            if batch is END_OF_FILE:
                raise EmptyFileError

            return batch
