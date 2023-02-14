# =============================================================================
# Casanova Utils
# =============================================================================
#
# Miscellaneous utility functions.
#
import re
import csv
import gzip
from io import StringIO


def ensure_open(p, encoding="utf-8", mode="r"):
    if not isinstance(p, str):
        return p

    if p.endswith(".gz"):
        if "b" in mode:
            return gzip.open(p, mode=mode)

        mode += "t"
        return gzip.open(p, encoding=encoding, mode=mode)

    if "b" in mode:
        return open(p, mode=mode)

    return open(p, encoding=encoding, mode=mode)


BOM_RE = re.compile(r"^\ufeff")


def suppress_BOM(string):
    return re.sub(BOM_RE, "", string)


def has_null_byte(string):
    return "\0" in string


def strip_null_bytes(string):
    return string.replace("\0", "")


def lines_without_null_bytes(iterable):
    for line in iterable:
        yield strip_null_bytes(line)


def first_cell_index_with_null_byte(row):
    for i, cell in enumerate(row):
        if has_null_byte(cell):
            return i

    return None


def rows_without_null_bytes(iterable):
    for row in iterable:
        if any(has_null_byte(cell) for cell in row):
            yield [strip_null_bytes(cell) for cell in row]

        else:
            yield row


def size_of_row_in_memory(row):
    """
    Returns the approximate amount of bytes needed to represent the given row into
    the python's program memory.

    The magic numbers are based on `sys.getsizeof`.
    """
    a = 64 + 8 * len(row)  # Size of the array
    a += sum(49 + len(cell) for cell in row)  # Size of the contained strings

    return a


def size_of_row_in_file(row):
    """
    Returns the approximate amount of bytes originally used to represent the
    given row in its CSV file. It assumes the delimiter uses only one byte.

    I also ignores quotes (-2 bytes) around escaped cells if they were
    originally present.

    I also don't think that it counts 16 bit chars correctly.
    """
    a = max(0, len(row) - 1)
    a += sum(len(cell) for cell in row)

    return a


def CsvCellIO(column, value):
    buf = StringIO()
    writer = csv.writer(buf, dialect=csv.unix_dialect, quoting=csv.QUOTE_MINIMAL)
    writer.writerow([column])
    writer.writerow([value])

    buf.seek(0)

    return buf


def CsvRowIO(columns, row):
    buf = StringIO()
    writer = csv.writer(buf, dialect=csv.unix_dialect, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(columns)
    writer.writerow(row)

    buf.seek(0)

    return buf
