# =============================================================================
# Casanova Monkey Reader
# =============================================================================
#
# A Casanova reader relying on csvmonkey for performance.
#
import csvmonkey

from casanova.reader import CasanovaReader, make_headers_namedtuple
from casanova.utils import is_binary_buffer
from casanova.exceptions import InvalidFileError, EmptyFileError


class CasanovaMonkeyReader(CasanovaReader):
    def __init__(self, input_file, no_headers=False):

        # Ensuring we are reading a binary buffer
        if not is_binary_buffer(input_file):
            raise InvalidFileError('casanova_monkey.reader: expecting file in binary mode (e.g. "rb") or BytesIO.')

        self.input_file = input_file
        self.reader = csvmonkey.from_file(input_file, header=False)
        self.fieldnames = None
        self.first_row = None
        self.can_slice = False
        self.binary = True

        if no_headers:
            try:
                self.first_row = next(self.reader)
            except StopIteration:
                raise EmptyFileError

            self.pos = make_headers_namedtuple(len(self.first_row))
        else:
            try:
                self.fieldnames = list(next(self.reader))
            except StopIteration:
                raise EmptyFileError

            self.pos = make_headers_namedtuple(self.fieldnames)
