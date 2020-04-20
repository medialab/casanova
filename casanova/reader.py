# =============================================================================
# Casanova Reader
# =============================================================================
#
# A fast but comfortable CSV reader based upon csv.reader to avoid dealing
# with csv.DictReader which is nice but very slow.
#
import csv
from collections import namedtuple

from casanova.exceptions import EmptyFileException


def make_headers_namedtuple(headers):
    class HeadersPositions(namedtuple('HeadersPositions', headers)):
        __slots__ = ()
        pass

    return HeadersPositions(*range(len(headers)))


class CasanovaReader(object):
    def __init__(self, input_file, no_headers=False):

        self.input_file = input_file
        self.reader = csv.reader(input_file)
        self.fieldnames = None
        self.current_row = None
        self.started = False

        if no_headers:
            try:
                self.current_row = next(self.reader)
            except StopIteration:
                raise EmptyFileException

            self.pos = make_headers_namedtuple(range(len(self.current_row)))
        else:
            try:
                self.fieldnames = next(self.reader)
            except StopIteration:
                raise EmptyFileException

            self.pos = make_headers_namedtuple(self.fieldnames)

    def __iter__(self):
        return iter(self.reader)
