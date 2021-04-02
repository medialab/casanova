# =============================================================================
# Casanova Reader
# =============================================================================
#
# A fast but comfortable CSV reader based upon csv.reader to avoid dealing
# with csv.DictReader which is nice but very slow.
#
import csv

from casanova.utils import is_contiguous, ensure_open, suppress_BOM, count_bytes_in_row
from casanova.exceptions import EmptyFileError, MissingColumnError


class DictLikeRow(object):
    __slots__ = ('__mapping', '__row')

    def __init__(self, mapping, row):
        self.__mapping = mapping
        self.__row = row

    def __getitem__(self, key):
        return self.__row[self.__mapping[key]]

    def __getattr__(self, key):
        return self.__getitem__(key)


class HeadersPositions(object):
    def __init__(self, headers):
        if isinstance(headers, int):
            self.__headers = list(range(headers))
            self.__mapping = {i: i for i in self.__headers}
        else:
            self.__headers = headers
            self.__mapping = {h: i for i, h in enumerate(self.__headers)}

    def __len__(self):
        return len(self.__headers)

    def __getitem__(self, key):
        return self.__mapping[key]

    def __getattr__(self, key):
        return self.__getitem__(key)

    def __contains__(self, key):
        return key in self.__mapping

    def __iter__(self):
        yield from self.__mapping.items()

    def as_dict(self):
        return self.__mapping.copy()

    def get(self, key, default=None):
        return self.__mapping.get(key, default)

    def wrap(self, row):
        return DictLikeRow(self.__mapping, row)

    def __repr__(self):
        class_name = self.__class__.__name__

        representation = '<' + class_name

        for h, i in self.__mapping.items():
            if h.isidentifier():
                representation += ' %s=%s' % (h, i)
            else:
                representation += ' "%s"=%s' % (h, i)

        representation += '>'

        return representation


def get_column_index(pos, key, default=None):
    try:
        return pos[key]
    except KeyError:
        return default


def collect_column_indices(pos, columns):
    indices = []

    for column in columns:
        i = get_column_index(pos, column)

        if i is None:
            raise MissingColumnError

        indices.append(i)

    return indices


class CasanovaReader(object):
    namespace = 'casanova.reader'

    def __init__(self, input_file, no_headers=False, encoding='utf-8',
                 dialect=None, quotechar=None, delimiter=None, buffer_up_to_bytes=None):

        # Should we open a file for the user?
        input_file = ensure_open(input_file, encoding=encoding)

        reader_kwargs = {}

        if dialect is not None:
            reader_kwargs['dialect'] = dialect
        if quotechar is not None:
            reader_kwargs['quotechar'] = quotechar
        if delimiter is not None:
            reader_kwargs['delimiter'] = delimiter

        self.input_file = input_file
        self.reader = csv.reader(input_file, **reader_kwargs)
        self.fieldnames = None
        self.buffered_rows = []
        self.was_completely_buffered = False
        self.can_slice = True
        self.binary = False

        if no_headers:
            try:
                self.buffered_rows.append(next(self.reader))
            except StopIteration:
                raise EmptyFileError

            self.pos = HeadersPositions(len(self.buffered_rows[0]))
        else:
            try:
                self.fieldnames = next(self.reader)

                if self.fieldnames:
                    self.fieldnames[0] = suppress_BOM(self.fieldnames[0])

            except StopIteration:
                raise EmptyFileError

            self.pos = HeadersPositions(self.fieldnames)

        if buffer_up_to_bytes is not None:
            if not isinstance(buffer_up_to_bytes, int) or buffer_up_to_bytes < 1:
                raise TypeError('expecting a positive integer as "buffer_up_to_bytes" kwarg')

            buffered_bytes = 0

            while buffered_bytes < buffer_up_to_bytes:
                row = next(self.reader, None)

                if row is None:
                    self.was_completely_buffered = True
                    break

                buffered_bytes = count_bytes_in_row(row)
                self.buffered_rows.append(row)

    def __repr__(self):
        columns_info = ' '.join('%s=%s' % t for t in zip(self.pos._fields, self.pos))

        return '<%s %s>' % (self.namespace, columns_info)

    def iter(self):
        yield from self.buffered_rows
        yield from self.reader

    def wrap(self, row):
        return self.pos.wrap(row)

    def __iter__(self):
        return self.iter()

    def __records(self, columns, with_rows=False):
        pos = collect_column_indices(self.pos, columns)

        if self.can_slice and is_contiguous(pos):
            if len(pos) == 1:
                s = slice(pos[0], pos[0] + 1)
            else:
                s = slice(pos[0], pos[1] + 1)

            if with_rows:
                def iterator():
                    for row in self.iter():
                        yield row, row[s]
            else:
                def iterator():
                    for row in self.iter():
                        yield row[s]
        else:
            if with_rows:
                def iterator():
                    for row in self.iter():
                        yield row, [row[i] for i in pos]
            else:
                def iterator():
                    for row in self.iter():
                        yield [row[i] for i in pos]

        return iterator()

    def __cells(self, column, with_rows=False):
        i = get_column_index(self.pos, column)

        if i is None:
            raise MissingColumnError(column)

        if with_rows:
            def iterator():
                for row in self.iter():
                    yield row, row[i]
        else:
            def iterator():
                for row in self.iter():
                    yield row[i]

        return iterator()

    def cells(self, column, with_rows=False):
        if not isinstance(column, (str, int)):
            return self.__records(column, with_rows=with_rows)

        return self.__cells(column, with_rows=with_rows)

    def close(self):
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
