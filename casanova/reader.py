# =============================================================================
# Casanova Reader
# =============================================================================
#
# A fast but comfortable CSV reader based upon csv.reader to avoid dealing
# with csv.DictReader which is nice but very slow.
#
import csv

from casanova.utils import is_contiguous, ensure_open, suppress_BOM
from casanova.exceptions import EmptyFileError, MissingColumnError


class HeadersPositions(object):
    def __init__(self, headers):
        if isinstance(headers, int):
            self.__headers = list(range(headers))
            self._dict = {i: i for i in self.__headers}
        else:
            self.__headers = headers
            self._dict = {h: i for i, h in enumerate(self.__headers)}

            for name, value in self._dict.items():
                if name.isidentifier():
                    setattr(self, name, value)

    def __len__(self):
        return len(self.__headers)

    def __getitem__(self, key):
        return self._dict[key]

    def __contains__(self, key):
        return key in self._dict

    def get(self, key, default=None):
        return self._dict.get(key, default)


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

    def __init__(self, input_file, no_headers=False, encoding='utf-8'):

        # Should we open a file for the user?
        input_file = ensure_open(input_file, encoding=encoding)

        self.input_file = input_file
        self.reader = csv.reader(input_file)
        self.fieldnames = None
        self.first_row = None
        self.can_slice = True
        self.binary = False

        if no_headers:
            try:
                self.first_row = next(self.reader)
            except StopIteration:
                raise EmptyFileError

            self.pos = HeadersPositions(len(self.first_row))
        else:
            try:
                self.fieldnames = next(self.reader)

                if self.fieldnames:
                    self.fieldnames[0] = suppress_BOM(self.fieldnames[0])

            except StopIteration:
                raise EmptyFileError

            self.pos = HeadersPositions(self.fieldnames)

    def __repr__(self):
        columns_info = ' '.join('%s=%s' % t for t in zip(self.pos._fields, self.pos))

        return '<%s %s>' % (self.namespace, columns_info)

    def iter(self):
        if self.first_row is not None:
            yield self.first_row
            self.first_row = None

        yield from self.reader

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
