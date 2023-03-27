# =============================================================================
# Casanova Headers
# =============================================================================
#
# Utility class representing a CSV file's headers
#
import re
from ebbe import with_next
from collections import namedtuple, defaultdict
from collections.abc import Iterable
from functools import wraps

from casanova.exceptions import (
    InvalidSelectionError,
    MissingColumnError,
    NthNamedColumnOutOfRangeError,
    UnknownNamedColumnError,
    ColumnOutOfRangeError,
)

PROJECTION_SHAPE_TYPES = (int, str, tuple, list, dict)


class Selection(object):
    def __init__(self, inverted=False):
        self.groups = []
        self.inverted = inverted

    def add(self, group):
        self.groups.append(group)

    def is_suitable_without_headers(self) -> bool:
        for group in self.groups:
            if isinstance(group, SingleColumn):
                if not isinstance(group.key, int):
                    return False

            elif isinstance(group, IndexedColumn):
                return False

            elif isinstance(group, ColumnRange):
                # NOTE: start and end cannot be mixed, so it does not
                # make sense to also test end
                if not isinstance(group.start, int):
                    return False

            else:
                return False

        return True

    def __iter__(self):
        yield from self.groups


SingleColumn = namedtuple("SingleColumn", ["key"])
ColumnRange = namedtuple("ColumnRange", ["start", "end"])
IndexedColumn = namedtuple("IndexedColumn", ["key", "index"])

INDEXED_HEADER_RE = re.compile(r"^.+\[(\d+)\]$")
INDEX_REPLACER_RE = re.compile(r"\[\d+\]$")


def redirect_errors_as_invalid_selection(fn):
    @wraps(fn)
    def wrapped(self, selection):
        try:
            return fn(self, selection)
        except MissingColumnError as e:
            raise InvalidSelectionError(selection=selection, reason=e)

    return wrapped


def parse_key(key):
    if not key:
        return None

    if key.isdigit():
        return int(key)

    return INDEX_REPLACER_RE.sub("", key)


def parse_selection(string):
    """
    From xsv:

    Select one column by name:
        * name

    Select one column by index (1-based):
        * 2

    Select the first and fourth columns:
        * 1,4

    Select the first 4 columns (by index and by name):
        * 1-4
        * Header1-Header4

    Ignore the first 2 columns (by range and by omission):
        * 3-
        * '!1-2'

    Select the third column named 'Foo':
        * 'Foo[2]'

    Re-order and duplicate columns arbitrarily:
        * 3-1,Header3-Header1,Header1,Foo[2],Header1

    Quote column names that conflict with selector syntax:
        * '"Date - Opening","Date - Actual Closing"'
    """
    inverted = False

    if string.startswith("!"):
        inverted = True
        string = string[1:]

    selection = Selection(inverted=inverted)

    def tokens():
        acc = ""
        current_escapechar = None
        escaping = False

        for c in string:
            if c == "\\":
                escaping = True
                continue

            if escaping:
                escaping = False
                acc += c
                continue

            if c == current_escapechar:
                current_escapechar = None
                continue

            if c == "'" or c == '"':
                current_escapechar = c
                continue

            if current_escapechar is None:
                if c == ",":
                    yield (acc, ",")
                    acc = ""
                    continue

                elif c == "-":
                    yield (acc, "-")
                    acc = ""
                    continue

            acc += c

        if acc:
            yield (acc, None)

    def combined_tokens():
        init = True
        skip_next = False

        for (
            (token, sep),
            next_item,
        ) in with_next(tokens()):
            if skip_next:
                skip_next = False
                continue

            if not init and inverted:
                raise InvalidSelectionError(
                    reason=TypeError("invalid-inverted-selection"),
                    selection=string,
                )

            key = parse_key(token)

            init = False

            if sep == "-":
                skip_next = True

                end_key = parse_key(next_item[0]) if next_item is not None else None

                if end_key is not None and type(key) is not type(end_key):
                    raise InvalidSelectionError(
                        reason=TypeError("mixed-range"), selection=string
                    )

                # NOTE: ranges are 1-based in xsv
                if isinstance(key, int):
                    key -= 1

                    if end_key is not None:
                        end_key -= 1

                    if key < 0 or (end_key is not None and end_key < 0):
                        raise InvalidSelectionError(
                            reason=TypeError("negative-index"), selection=string
                        )

                if end_key is not None and key == end_key:
                    yield SingleColumn(key=key)
                    continue

                yield ColumnRange(start=key, end=end_key)
                continue

            index_match = INDEXED_HEADER_RE.match(token)

            if index_match:
                if isinstance(key, int):
                    raise InvalidSelectionError(
                        reason=TypeError("invalid-indexation"), selection=string
                    )

                yield IndexedColumn(key=key, index=int(index_match.group(1)))
                continue

            if isinstance(key, int):
                key -= 1

                if key < 0:
                    raise InvalidSelectionError(
                        reason=TypeError("negative-index"), selection=string
                    )

            yield SingleColumn(key=key)

    for group in combined_tokens():
        selection.add(group)

    return selection


def walk_shape(node, headers):
    if isinstance(node, (int, str)):
        return headers[node]

    if isinstance(node, list):
        return [walk_shape(n, headers) for n in node]

    if isinstance(node, tuple):
        return tuple(walk_shape(n, headers) for n in node)

    if isinstance(node, dict):
        o = {}

        for k, v in node.items():
            o[k] = walk_shape(v, headers)

        return o

    raise NotImplementedError


def walk_row(node, row):
    if isinstance(node, (int)):
        return row[node]

    if isinstance(node, list):
        return [walk_row(n, row) for n in node]

    if isinstance(node, tuple):
        return tuple(walk_row(n, row) for n in node)

    if isinstance(node, dict):
        o = {}

        for k, v in node.items():
            o[k] = walk_row(v, row)

        return o

    raise NotImplementedError


class RowWrapper(object):
    __slots__ = ("__headers", "__row")

    def __init__(self, headers, row):
        self.__headers = headers
        self.__row = row

    def _replace(self, row):
        self.__row = row

    def __getitem__(self, key):
        return self.__row[self.__headers[key]]

    def get(self, key, default=None, index=None):
        idx = self.__headers.get(key, index=index)

        if idx is None:
            return default

        return self.__row[idx]

    def __contains__(self, key):
        return key in self.__headers

    def __getattr__(self, key):
        return self.__getitem__(key)

    def __iter__(self):
        yield from self.__row

    def __len__(self):
        return len(self.__row)

    def cells(self):
        yield from zip(self.__headers.fieldnames, self.__row)


class Headers(object):
    def __init__(self, fieldnames):
        self.fieldnames = list(fieldnames)

        self.__mapping = defaultdict(list)
        self.__flat_mapping = {}
        self.__wrapper = RowWrapper(self, fieldnames)

        for i, h in enumerate(self.fieldnames):
            self.__mapping[h].append(i)
            self.__flat_mapping[h] = i

    def __eq__(self, other):
        return self.fieldnames == other.fieldnames

    def __len__(self):
        return len(self.fieldnames)

    def __getitem__(self, key):
        if isinstance(key, int):
            if key >= len(self):
                raise ColumnOutOfRangeError(key)

            return key

        if isinstance(key, tuple):
            if len(key) != 2:
                raise TypeError("expecting a str, a int or a (str, int) tuple")

            indices = self.__mapping.get(key[0])

            if indices is None:
                raise UnknownNamedColumnError(key[0])

            try:
                return indices[key[1]]
            except IndexError:
                raise NthNamedColumnOutOfRangeError(key[0], key[1])

        indices = self.__mapping.get(key)

        if indices is None:
            raise UnknownNamedColumnError(key)

        assert len(indices) > 0

        return indices[0]

    def __getattr__(self, key):
        return self.__getitem__(key)

    def __contains__(self, key):
        if isinstance(key, int):
            return key < len(self)

        if isinstance(key, tuple):
            if len(key) != 2:
                raise TypeError("expecting a str, a int or a (str, int) tuple")

            indices = self.__mapping.get(key[0])

            if indices is None:
                return False

            return key[1] < len(indices)

        return key in self.__mapping

    def __iter__(self):
        yield from self.fieldnames

    def nth(self, index):
        try:
            return self.fieldnames[index]
        except IndexError:
            raise ColumnOutOfRangeError(index)

    def get(self, key, default=None, index=None):
        if isinstance(key, tuple):
            if len(key) != 2:
                raise TypeError("expecting a str, a int or a (str, int) tuple")

            if index is not None:
                raise TypeError(
                    "key cannot be a (name, index) tuple if index kwarg is not None"
                )

            index = key[1]
            key = key[0]

        if isinstance(key, int):
            if index is not None:
                raise TypeError("it does not make sense to get an nth index")

            if key >= len(self):
                return default

            return key

        indices = self.__mapping.get(key)

        if indices is None:
            return default

        assert len(indices) > 0

        if index is not None:
            return indices[index]

        return indices[0]

    @redirect_errors_as_invalid_selection
    def select(self, selection):
        indices = []

        if not isinstance(selection, (str, Selection)):
            if not isinstance(selection, Iterable):
                raise TypeError(
                    "invalid selection. expecting str, Selection or iterable"
                )

            for key in selection:
                indices.append(self[key])

            return indices

        parsed_selection = (
            parse_selection(selection)
            if not isinstance(selection, Selection)
            else selection
        )

        for group in parsed_selection:
            if isinstance(group, SingleColumn):
                key = self[group.key]

                if parsed_selection.inverted:
                    for i in range(len(self)):
                        if i == key:
                            continue

                        indices.append(i)
                else:
                    indices.append(key)

            elif isinstance(group, IndexedColumn):
                key = self[group.key, group.index]

                if parsed_selection.inverted:
                    for i in range(len(self)):
                        if i == key:
                            continue

                        indices.append(i)
                else:
                    indices.append(key)

            elif isinstance(group, ColumnRange):
                # NOTE: ranges are all inclusive in xsv dsl
                start = self[group.start]
                end = self[group.end] if group.end is not None else None

                target_range: range

                if end is not None:
                    if start > end:
                        target_range = range(start, end - 1, -1)
                    else:
                        target_range = range(start, end + 1)
                else:
                    target_range = range(start, len(self))

                if parsed_selection.inverted:
                    for i in range(len(self)):
                        if i in target_range:
                            continue

                        indices.append(i)
                else:
                    indices.extend(list(target_range))

            else:
                raise NotImplementedError(
                    "selection implementation is erroneously not exhaustive"
                )

        return indices

    def project(self, shape):
        if not isinstance(shape, PROJECTION_SHAPE_TYPES):
            raise TypeError("invalid projection shape")

        indexed_shape = walk_shape(shape, self)

        def projection(row):
            return walk_row(indexed_shape, row)

        return projection

    def flat_project(self, *args):
        if len(args) < 1:
            raise TypeError("not enough arguments")

        if len(args) > 1:
            shape = tuple(args)
        else:
            shape = args[0]

        if not isinstance(shape, PROJECTION_SHAPE_TYPES):
            raise TypeError("invalid projection shape")

        def select_one(target):
            if isinstance(target, int):
                return target

            result = self.select(target)

            if len(result) > 1:
                raise TypeError(
                    "projection shape includes a selection returning more than one column"
                )

            return result[0]

        if isinstance(shape, (int, str)):
            idx = select_one(shape)

            def projection(row):
                return row[idx]

        elif isinstance(shape, tuple):
            indices = [select_one(item) for item in shape]

            def projection(row):
                return tuple(row[i] for i in indices)

        elif isinstance(shape, list):
            indices = [select_one(item) for item in shape]

            def projection(row):
                return [row[i] for i in indices]

        elif isinstance(shape, dict):
            indices = {k: select_one(v) for k, v in shape.items()}

            def projection(row):
                return {k: row[v] for k, v in indices.items()}

        else:
            raise NotImplementedError

        return projection

    def wrap(self, row, transient=False):
        if len(row) != len(self.fieldnames):
            raise TypeError("len mismatch for row and headers")

        if transient:
            self.__wrapper._replace(row)
            return self.__wrapper

        return RowWrapper(self, row)

    def __repr__(self):
        class_name = self.__class__.__name__

        representation = "<" + class_name

        for i, h in enumerate(self):
            if h.isidentifier():
                representation += " %s=%s" % (h, i)
            else:
                representation += ' "%s"=%s' % (h, i)

        representation += ">"

        return representation

    @classmethod
    def select_no_headers(cls, count, selection):
        if isinstance(selection, str):
            parsed_selection = parse_selection(selection)

            if not parsed_selection.is_suitable_without_headers():
                raise InvalidSelectionError(
                    selection=selection,
                    reason=TypeError("irrelevant-naming-no-headers"),
                )

            selection = parsed_selection

        headers = cls(range(count))

        return headers.select(selection)

    @classmethod
    def flat_project_no_headers(cls, count, *shape):
        headers = cls(range(count))

        return headers.flat_project(*shape)

    @classmethod
    def rename(cls, headers: "Headers", old_name: str, new_name: str):
        new_fieldnames = []

        for f in headers.fieldnames:
            new_fieldnames.append(f if f != old_name else new_name)

        return cls(new_fieldnames)
