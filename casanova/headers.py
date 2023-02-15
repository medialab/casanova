# =============================================================================
# Casanova Headers
# =============================================================================
#
# Utility class representing a CSV file's headers
#
import re
from ebbe import with_next
from collections import namedtuple, defaultdict

from casanova.exceptions import InvalidSelectionError

SimpleSelection = namedtuple("SimpleSelection", ["key", "negative"], defaults=[False])
RangeSelection = namedtuple(
    "RangeSelection", ["start", "end", "negative"], defaults=[False]
)
IndexedSelection = namedtuple(
    "IndexedSelection", ["key", "index", "negative"], defaults=[False]
)

INDEXED_HEADER_RE = re.compile(r"^.+\[(\d+)\]$")
INDEX_REPLACER_RE = re.compile(r"\[\d+\]$")


def parse_key(key):
    if not key:
        return None

    if key.startswith("!"):
        key = key[1:]

    if key.isdigit():
        return int(key)

    return INDEX_REPLACER_RE.sub("", key)


# TODO: in escape backslash
def parse_selection(selection):
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

    def parts():
        acc = ""
        current_escapechar = None

        for c in selection:
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

    def combined_parts():
        init = True
        skip_next = False

        for (
            (part, sep),
            next_item,
        ) in with_next(parts()):
            if skip_next:
                skip_next = False
                continue

            negative = part.startswith("!")

            if not init and negative:
                raise InvalidSelectionError(
                    "negative selection can only have one part", selection=selection
                )

            key = parse_key(part)

            init = False

            if sep == "-":
                skip_next = True

                end_key = parse_key(next_item[0]) if next_item is not None else None

                if end_key is not None and type(key) is not type(end_key):
                    raise InvalidSelectionError(
                        "range selection should not be mixed", selection=selection
                    )

                # NOTE: fixing 1-based
                if isinstance(key, int):
                    key -= 1

                    if end_key is not None:
                        end_key -= 1

                    if key < 0 or (end_key is not None and end_key < 0):
                        raise InvalidSelectionError("range has negative index")

                if end_key is not None and key == end_key:
                    yield SimpleSelection(key=key, negative=negative)
                    continue

                yield RangeSelection(
                    start=key,
                    end=end_key,
                    negative=negative,
                )
                continue

            index_match = INDEXED_HEADER_RE.match(part)

            if index_match:
                if isinstance(key, int):
                    raise InvalidSelectionError(
                        "indexed selection cannot be numerical", selection=selection
                    )

                yield IndexedSelection(
                    key=key,
                    index=int(index_match.group(1)),
                    negative=negative,
                )
                continue

            yield SimpleSelection(key=key, negative=negative)

    yield from combined_parts()


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

    def __len__(self):
        return len(self.fieldnames)

    def __getitem__(self, key):
        # NOTE: for numeric keys (i.e. no headers), this function
        # is identity
        if isinstance(key, int):
            if key >= len(self.fieldnames):
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

    def get(self, key, default=None, index=None):
        indices = self.__mapping.get(key)

        if indices is None:
            return default

        assert len(indices) > 0

        if index is not None:
            return indices[index]

        return indices[0]

    def select(self, selection):
        # TODO: maybe we don't need to accumulate slices
        # TODO: handle simple case when selection is a simple integer

        slices = []

        for item in parse_selection(selection):
            if item.negative:
                raise NotImplementedError

            if isinstance(item, SimpleSelection):
                if isinstance(item.key, int):
                    slices.append(item.key)
                else:
                    slices.append(self[item.key])

            elif isinstance(item, RangeSelection):
                start = item.start
                end = item.end

                if not isinstance(start, int):
                    start = self[start]

                    if end is not None:
                        end = self[end]

                if end is not None and start > end:
                    slices.append(slice(start, end, -1))
                else:
                    slices.append(slice(start, end))
            else:
                idx = self.get(item.key, index=item.index)

                if idx is None:
                    raise KeyError("%s[%i]" % item)

                slices.append(idx)

        all_indices = list(range(len(self)))
        indices = []

        for i in slices:
            if isinstance(i, slice):
                indices.extend(all_indices[i])
            else:
                indices.append(all_indices[i])

        return indices

    def collect(self, keys):
        return [self[k] for k in keys]

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
