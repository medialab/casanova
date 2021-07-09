# =============================================================================
# Casanova Reader
# =============================================================================
#
# A fast but comfortable CSV reader based upon csv.reader to avoid dealing
# with csv.DictReader which is nice but very slow.
#
import csv
from collections import deque
from collections.abc import Iterable
from io import IOBase
from operator import itemgetter

from casanova.defaults import DEFAULTS
from casanova.utils import ensure_open, suppress_BOM, size_of_row_in_file
from casanova.exceptions import EmptyFileError, MissingColumnError, NoHeadersError


def validate_multiplex_tuple(multiplex):
    return (
        isinstance(multiplex, tuple) and
        len(multiplex) in [2, 3] and
        all(isinstance(t, str) for t in multiplex)
    )


class DictLikeRow(object):
    __slots__ = ('__mapping', '__row')

    def __init__(self, mapping, row):
        self.__mapping = mapping
        self.__row = row

    def __getitem__(self, key):
        return self.__row[self.__mapping[key]]

    def __getattr__(self, key):
        return self.__getitem__(key)


class Headers(object):
    def __init__(self, fieldnames):
        self.__mapping = {h: i for i, h in enumerate(fieldnames)}

    def rename(self, old_name, new_name):
        if old_name == new_name:
            raise TypeError

        self.__mapping[new_name] = self[old_name]
        del self.__mapping[old_name]

    def __len__(self):
        return len(self.__mapping)

    def __getitem__(self, key):
        return self.__mapping[key]

    def __getattr__(self, key):
        return self.__getitem__(key)

    def __contains__(self, key):
        return key in self.__mapping

    def __iter__(self):
        yield from sorted(self.__mapping.items(), key=itemgetter(1))

    def as_dict(self):
        return self.__mapping.copy()

    def get(self, key, default=None):
        return self.__mapping.get(key, default)

    def collect(self, keys):
        return [self[k] for k in keys]

    def wrap(self, row):
        return DictLikeRow(self.__mapping, row)

    def __repr__(self):
        class_name = self.__class__.__name__

        representation = '<' + class_name

        for h, i in self:
            if h.isidentifier():
                representation += ' %s=%s' % (h, i)
            else:
                representation += ' "%s"=%s' % (h, i)

        representation += '>'

        return representation


class Reader(object):
    namespace = 'casanova.reader'

    def __init__(self, input_file, no_headers=False, encoding='utf-8',
                 dialect=None, quotechar=None, delimiter=None, prebuffer_bytes=None,
                 total=None, multiplex=None):

        # Resolving global defaults
        if prebuffer_bytes is None:
            prebuffer_bytes = DEFAULTS['prebuffer_bytes']

        # Detecting input type
        if isinstance(input_file, IOBase):
            input_type = 'file'

        elif isinstance(input_file, str):
            input_type = 'path'
            input_file = ensure_open(input_file, encoding=encoding)

        elif isinstance(input_file, Iterable):
            input_type = 'iterable'
            input_file = iter(input_file)

        else:
            raise TypeError('expecting a file, a path or an iterable of rows')

        if multiplex is not None and not validate_multiplex_tuple(multiplex):
            raise TypeError('`multiplex` should be a 2-tuple or 3-tuple containing the column to split, the split character and optionally a new name for the column')

        reader_kwargs = {}

        if dialect is not None:
            reader_kwargs['dialect'] = dialect
        if quotechar is not None:
            reader_kwargs['quotechar'] = quotechar
        if delimiter is not None:
            reader_kwargs['delimiter'] = delimiter

        self.input_type = input_type
        self.input_file = input_file

        if self.input_type == 'iterable':
            self.reader = self.input_file
        else:
            self.reader = csv.reader(input_file, **reader_kwargs)

        self.buffered_rows = deque()
        self.was_completely_buffered = False
        self.total = total
        self.headers = None
        self.expected_row_length = None

        # Reading headers
        if no_headers:
            try:
                self.buffered_rows.append(next(self.reader))
            except StopIteration:
                raise EmptyFileError

            self.expected_row_length = len(self.buffered_rows[0])
        else:
            try:
                fieldnames = next(self.reader)

                if fieldnames:
                    fieldnames[0] = suppress_BOM(fieldnames[0])

            except StopIteration:
                raise EmptyFileError

            self.headers = Headers(fieldnames)

        # Multiplexing
        if multiplex is not None:
            multiplex_column = multiplex[0]
            split_char = multiplex[1]

            if multiplex_column not in self.headers:
                raise MissingColumnError(multiplex_column)

            multiplex_pos = self.headers[multiplex_column]

            # New col
            if len(multiplex) == 3:
                self.headers.rename(multiplex_column, multiplex[2])

            original_reader = self.reader

            def reader_wrapper():
                for row in original_reader:
                    cell = row[multiplex_pos]

                    if not cell or split_char not in cell:
                        yield row

                    else:
                        for value in cell.split(split_char):
                            copy = list(row)
                            copy[multiplex_pos] = value
                            yield copy

            self.reader = reader_wrapper()

        # Prebuffering
        if prebuffer_bytes is not None and self.total is None:
            if not isinstance(prebuffer_bytes, int) or prebuffer_bytes < 1:
                raise TypeError('expecting a positive integer as "prebuffer_bytes" kwarg')

            buffered_bytes = 0

            while buffered_bytes < prebuffer_bytes:
                row = next(self.reader, None)

                if row is None:
                    self.was_completely_buffered = True
                    self.total = len(self.buffered_rows)
                    break

                buffered_bytes += size_of_row_in_file(row)
                self.buffered_rows.append(row)

    def __repr__(self):
        columns_info = ' '.join('%s=%s' % t for t in self.headers)

        return '<%s %s>' % (self.namespace, columns_info)

    @property
    def fieldnames(self):
        if self.headers is None:
            return None

        return [k for k, v in self.headers]

    @property
    def row_len(self):
        if self.expected_row_length is not None:
            return self.expected_row_length

        return len(self.headers)

    def iter(self):
        while self.buffered_rows:
            yield self.buffered_rows.popleft()

        yield from self.reader

    def wrap(self, row):
        return self.headers.wrap(row)

    def __iter__(self):
        return self.iter()

    def __cells(self, column, with_rows=False):
        if not isinstance(column, int):
            if self.headers is None:
                raise NoHeadersError

            pos = self.headers.get(column)

            if pos is None:
                raise MissingColumnError(column)
        else:
            if column >= self.row_len:
                raise MissingColumnError

            pos = column

        if with_rows:
            def iterator():
                for row in self.iter():
                    yield row, row[pos]
        else:
            def iterator():
                for row in self.iter():
                    yield row[pos]

        return iterator()

    def cells(self, column, with_rows=False):
        if not isinstance(column, (str, int)):
            raise TypeError

        return self.__cells(column, with_rows=with_rows)

    def close(self):
        if self.input_type == 'file':
            self.input_file.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @classmethod
    def count(cls, input_file, max_rows=None, **kwargs):
        assert max_rows is None or max_rows > 0, '%s.count: expected max_rows to be `None` or > 0.' % cls.namespace

        n = 0

        with cls(input_file, **kwargs) as reader:
            for _ in reader:
                n += 1

                if max_rows is not None and n > max_rows:
                    return None

        return n
