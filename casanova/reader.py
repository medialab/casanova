# =============================================================================
# Casanova Reader
# =============================================================================
#
# A fast but comfortable CSV reader based upon csv.reader to avoid dealing
# with csv.DictReader which is nice but very slow.
#
import csv
from collections import namedtuple

from casanova.utils import is_contiguous
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

        def __contains__(self, key):
            try:
                self[key]
                return True
            except (IndexError, KeyError):
                return False

    return HeadersPositions(*range(len(headers)))


class CasanovaReader(object):
    __slots__ = (
        'can_slice',
        'current_row',
        'fieldnames',
        'input_file',
        'pos',
        'reader',
        'started'
    )

    def __init__(self, input_file, no_headers=False):

        self.input_file = input_file
        self.reader = csv.reader(input_file)
        self.fieldnames = None
        self.current_row = None
        self.started = False
        self.can_slice = True

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

    def __repr__(self):
        columns_info = ' '.join('%s=%s' % t for t in zip(self.pos._fields, self.pos))

        return '<%s %s>' % (namespace, columns_info)

    def __iter__(self):
        if self.fieldnames is None and not self.started:
            yield self.current_row

        self.started = True

        for row in self.reader:
            yield row

    def cells(self, column):
        if not isinstance(column, (str, int)):
            return self.records(column)

        try:
            pos = self.pos[column]
        except (IndexError, KeyError):
            raise MissingHeaderError(column)

        def iterator():
            for row in self:
                yield row[pos]

        return iterator()

    def records(self, columns):
        pos = []

        for column in columns:
            try:
                i = self.pos[column]
            except (IndexError, KeyError):
                raise MissingHeaderError(column)

            pos.append(i)

        pos.sort()

        if self.can_slice and is_contiguous(pos):
            if len(pos) == 1:
                s = slice(pos[0], pos[0] + 1)
            else:
                s = slice(pos[0], pos[1] + 1)

            def iterator():
                for row in self:
                    yield row[s]
        else:
            def iterator():
                for row in self:
                    yield [row[i] for i in pos]

        return iterator()
