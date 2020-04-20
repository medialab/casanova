# =============================================================================
# Casanova Reader
# =============================================================================
#
# A fast but comfortable CSV reader based upon csv.reader to avoid dealing
# with csv.DictReader which is nice but very slow.
#
import csv
from collections import namedtuple

from casanova.exceptions import EmptyFileError, MissingHeaderError


def make_headers_namedtuple(headers):
    if isinstance(headers, int):
        return list(range(headers))

    class HeadersPositions(namedtuple('HeadersPositions', headers)):
        __slots__ = ()

        def __getitem__(self, key):
            if isinstance(key, int):
                return super().__getitem__(key)

            try:
                return getattr(self, key)
            except AttributeError:
                raise KeyError

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
                raise EmptyFileError

            self.pos = make_headers_namedtuple(len(self.current_row))
        else:
            try:
                self.fieldnames = next(self.reader)
            except StopIteration:
                raise EmptyFileError

            self.pos = make_headers_namedtuple(self.fieldnames)

    def __iter__(self):
        if self.fieldnames is None and not self.started:
            yield self.current_row

        self.started = True

        for row in self.reader:
            yield row

    def cells(self, column):
        try:
            pos = self.pos[column]
        except (IndexError, KeyError):
            raise MissingHeaderError

        def iterator():
            for row in self:
                yield row[pos]

        return iterator()
