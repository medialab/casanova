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
from casanova.utils import lookahead


class CasanovaReverseReader(CasanovaReader):
    namespace = 'casanova.reverse_reader'

    def __init__(self, input_file, **kwargs):
        super().__init__(input_file, **kwargs)

        self.backwards_file = open(input_file.name, 'rb')

        backwards_iterator = FileReadBackwardsIterator(
            self.backwards_file,
            input_file.encoding,
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
        self.input_file.close()
        self.backwards_file.close()
