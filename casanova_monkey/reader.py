# =============================================================================
# Casanova Monkey Reader
# =============================================================================
#
# A Casanova reader relying on csvmonkey for performance.
#
import csvmonkey

from casanova.reader import CasanovaReader, HeadersPositions
from casanova.utils import is_binary_buffer, ensure_open, suppress_BOM
from casanova.exceptions import InvalidFileError, EmptyFileError


class CasanovaMonkeyReader(CasanovaReader):
    namespace = 'casanova_monkey.reader'

    def __init__(self, input_file, no_headers=False, encoding='utf-8', lazy=False):

        # Should we open a file for the user?
        input_file = ensure_open(input_file, encoding=encoding, mode='rb')

        # Ensuring we are reading a binary buffer
        if not is_binary_buffer(input_file):
            raise InvalidFileError('casanova_monkey.reader: expecting file in binary mode (e.g. "rb") or BytesIO.')

        self.input_file = input_file
        self.reader = csvmonkey.from_file(input_file, header=False)
        self.fieldnames = None
        self.first_row = None
        self.can_slice = not lazy
        self.binary = True
        self.lazy = lazy

        if no_headers:
            try:
                self.first_row = next(self.reader)
            except StopIteration:
                raise EmptyFileError

            self.pos = HeadersPositions(len(self.first_row))
        else:
            try:
                self.fieldnames = next(self.reader).aslist()

                if self.fieldnames:
                    self.fieldnames[0] = suppress_BOM(self.fieldnames[0])

            except StopIteration:
                raise EmptyFileError

            self.pos = HeadersPositions(self.fieldnames)

    def iter(self):
        if self.first_row is not None:
            if self.lazy:
                yield self.first_row
            else:
                yield self.first_row.aslist()
            self.first_row = None

        if self.lazy:
            yield from self.reader
        else:
            for row in self.reader:
                yield row.aslist()
