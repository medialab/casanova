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

from casanova.exceptions import InvalidSelectionError


class Selection(object):
    def __init__(self, inverted=False):
        self.groups = []
        self.inverted = inverted

    def add(self, group):
        self.groups.append(group)

    def __iter__(self):
        yield from self.groups


SingleColumn = namedtuple("SingleColumn", ["key"])
ColumnRange = namedtuple("ColumnRange", ["start", "end"])
IndexedColumn = namedtuple("IndexedColumn", ["key", "index"])

INDEXED_HEADER_RE = re.compile(r"^.+\[(\d+)\]$")
INDEX_REPLACER_RE = re.compile(r"\[\d+\]$")


def parse_key(key):
    if not key:
        return None

    if key.isdigit():
        return int(key)

    return INDEX_REPLACER_RE.sub("", key)


# TODO: in escape backslash
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

        for c in string:
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

            yield SingleColumn(key=key)

    for group in combined_tokens():
        selection.add(group)

    return selection


class DictLikeRow(object):
    __slots__ = ("__mapping", "__row")

    def __init__(self, mapping, row):
        self.__mapping = mapping
        self.__row = row

    def __getitem__(self, key):
        return self.__row[self.__mapping[key]]

    def __getattr__(self, key):
        return self.__getitem__(key)


class Headers(object):
    def __init__(self, fieldnames):
        self.__mapping = defaultdict(list)
        self.__flat_mapping = {}
        self.fieldnames = fieldnames

        for i, h in enumerate(fieldnames):
            self.__mapping[h].append(i)
            self.__flat_mapping[h] = i

    def rename(self, old_name, new_name):
        new_fieldnames = list(self.fieldnames)

        for i, f in enumerate(new_fieldnames):
            if f == old_name:
                new_fieldnames[i] = new_name

        self.__init__(new_fieldnames)

    def __eq__(self, other):
        return self.fieldnames == other.fieldnames

    def __len__(self):
        return len(self.fieldnames)

    def __getitem__(self, key):
        if isinstance(key, int):
            if key >= len(self):
                raise IndexError(key)

            return key

        indices = self.__mapping.get(key)

        if indices is None:
            raise KeyError(key)

        assert len(indices) > 0

        return indices[0]

    def __getattr__(self, key):
        return self.__getitem__(key)

    def __contains__(self, key):
        return key in self.__mapping

    def __iter__(self):
        yield from self.fieldnames

    def nth(self, index):
        return self.fieldnames[index]

    def get(self, key, default=None, index=None):
        if isinstance(key, int):
            if index is not None:
                raise TypeError("it does not make sense to get an nth index")

            if key >= len(self):
                return default

        indices = self.__mapping.get(key)

        if indices is None:
            return default

        assert len(indices) > 0

        if index is not None:
            return indices[index]

        return indices[0]

    def select(self, selection):
        # TODO: all this should be wrapped to catch IndexError and KeyError to have
        # a selection error
        indices = []

        if not isinstance(selection, str):
            if not isinstance(selection, Iterable):
                raise TypeError("invalid selection. expecting str or iterable")

            for key in selection:
                indices.append(self[key])

            return indices

        try:
            parsed_selection = parse_selection(selection)

            for group in parsed_selection:
                if parsed_selection.inverted:
                    if isinstance(group, SingleColumn):
                        key = group.key

                        if isinstance(key, int):
                            if key >= len(self):
                                raise IndexError(key)
                        else:
                            key = self[key]

                        for i in range(len(self)):
                            if i == key:
                                continue

                            indices.append(i)
                    else:
                        raise NotImplementedError

                    continue

                if isinstance(group, SingleColumn):
                    if isinstance(group.key, int):
                        if group.key >= len(self):
                            raise IndexError(group.key)

                        indices.append(group.key)
                    else:
                        indices.append(self[group.key])

                elif isinstance(group, ColumnRange):
                    start = group.start
                    end = group.end

                    if not isinstance(start, int):
                        start = self[start]

                        if end is not None:
                            end = self[end]
                    else:
                        if start >= len(self):
                            raise IndexError(start)

                        if end is not None and end >= len(self):
                            raise IndexError(end)

                    if end is not None:
                        # NOTE: ranges are all inclusive
                        if start > end:
                            indices.extend(list(range(start, end - 1, -1)))
                        else:
                            indices.extend(list(range(start, end + 1)))

                    else:
                        indices.extend(list(range(start, len(self))))
                else:
                    idx = self.get(group.key, index=group.index)

                    if idx is None:
                        raise KeyError("%s[%i]" % group)

                    indices.append(idx)

        except (IndexError, KeyError) as e:
            raise InvalidSelectionError(reason=e, selection=selection)

        return indices

    def wrap(self, row):
        if len(row) != len(self.fieldnames):
            raise TypeError("len mismatch for row and headers")

        return DictLikeRow(self.__flat_mapping, row)

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
