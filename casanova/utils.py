# =============================================================================
# Casanova Utils
# =============================================================================
#
# Miscellaneous utility functions.
#
from io import BytesIO, BufferedReader, TextIOWrapper


def iter_with_prev(iterator):
    prev = None

    for item in iterator:
        if prev is not None:
            yield prev, item

        prev = item


def is_contiguous(l):
    for p, n in iter_with_prev(l):
        if p != n - 1:
            return False

    return True


def is_binary_buffer(buf):
    if isinstance(buf, BufferedReader):
        if 'b' not in buf.mode:
            return False
    elif not isinstance(buf, BytesIO):
        return False

    return True


def is_resumable_buffer(buf):
    if not isinstance(buf, (BufferedReader, TextIOWrapper)):
        return False

    if 'a' not in buf.mode and '+' not in buf.mode:
        return False

    return True


def is_empty_buffer(buf):
    return buf.tell() == 0
