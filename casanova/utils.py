# =============================================================================
# Casanova Utils
# =============================================================================
#
# Miscellaneous utility functions.
#


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
