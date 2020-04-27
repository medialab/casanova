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

    def __init__(self, input_file, no_headers=False, **kwargs):
        if no_headers:
            raise TypeError('casanova.reverse_reader: no_headers is not yet implemented for the reverse reader.')

        super().__init__(input_file, **kwargs)

        self.backwards_file = open(input_file.name, 'rb')

        backwards_iterator = FileReadBackwardsIterator(
            open(input_file.name, 'rb'),
            input_file.encoding,
            DEFAULT_BUFFER_SIZE
        )

        backwards_reader = csv.reader(backwards_iterator)

        def generator():

            for row, has_more in lookahead(backwards_reader):
                if has_more:
                    yield row

            self.close()

        self.reader = generator()

    def close(self):
        self.backwards_file.close()
