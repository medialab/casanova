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

from casanova.reader import CasanovaReader
from casanova.utils import lookahead, ensure_open
from casanova.exceptions import EmptyFileError


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

            for row, has_more in lookahead(backwards_reader):
                if has_more or self.fieldnames is None:
                    yield row

            self.close()

        self.reader = generator()

        if self.fieldnames is None:
            self.first_row = None

    def close(self):
        super().close()
        self.backwards_file.close()

    @staticmethod
    def last_cell(input_file, column, **kwargs):
        with CasanovaReverseReader(input_file, **kwargs) as reader:
            try:
                for record in reader.cells(column):
                    return record
            except StopIteration:
                raise EmptyFileError
