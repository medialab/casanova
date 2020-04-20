import csvmonkey
from io import BytesIO, BufferedReader
from casanova.reader import CasanovaReader, make_headers_namedtuple
from casanova.exceptions import InvalidFileError, EmptyFileError

binary_error = InvalidFileError('casanova_monkey.reader: expecting file in binary mode (e.g. "rb") or BytesIO.')


class CasanovaMonkeyReader(CasanovaReader):
    def __init__(self, input_file, no_headers=False):

        # Ensuring we are reading a binary buffer
        if isinstance(input_file, BufferedReader):
            if 'b' not in input_file.mode:
                raise binary_error
        elif not isinstance(input_file, BytesIO):
            raise binary_error

        self.input_file = input_file
        self.reader = csvmonkey.from_file(input_file, header=False)
        self.fieldnames = None
        self.current_row = None
        self.started = False
        self.can_slice = False

        if no_headers:
            try:
                self.current_row = next(self.reader)
            except StopIteration:
                raise EmptyFileError

            self.pos = make_headers_namedtuple(len(self.current_row))
        else:
            try:
                self.fieldnames = list(next(self.reader))
            except StopIteration:
                raise EmptyFileError

            self.pos = make_headers_namedtuple(self.fieldnames)
