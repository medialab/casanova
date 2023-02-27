# =============================================================================
# Casanova Global Defaults
# =============================================================================
#
# Global mutable defaults used by casanova classes.
#
from contextlib import contextmanager


class CasanovaDefaults(object):
    __slots__ = [
        "prebuffer_bytes",
        "strip_null_bytes_on_read",
        "strip_null_bytes_on_write",
    ]

    def __init__(self):
        self.prebuffer_bytes = None
        self.strip_null_bytes_on_read = False
        self.strip_null_bytes_on_write = False

    def save(self):
        return {n: getattr(self, n) for n in self.__slots__}

    def load(self, o):
        for n, v in o.items():
            setattr(self, n, v)


DEFAULTS = CasanovaDefaults()
NOT_GIVEN = object()


def set_defaults(
    prebuffer_bytes=NOT_GIVEN,
    strip_null_bytes_on_read=NOT_GIVEN,
    strip_null_bytes_on_write=NOT_GIVEN,
    plural_separator=NOT_GIVEN,
    none_value=NOT_GIVEN,
    true_value=NOT_GIVEN,
    false_value=NOT_GIVEN,
):
    global DEFAULTS

    if prebuffer_bytes is not NOT_GIVEN:
        if prebuffer_bytes is not None and (
            not isinstance(prebuffer_bytes, int) or prebuffer_bytes < 1
        ):
            raise TypeError("prebuffer_bytes should be None or a positive integer")

        DEFAULTS.prebuffer_bytes = prebuffer_bytes

    if strip_null_bytes_on_read is not NOT_GIVEN:
        if not isinstance(strip_null_bytes_on_read, bool):
            raise TypeError("strip_null_bytes_on_read should be a boolean")

        DEFAULTS.strip_null_bytes_on_read = strip_null_bytes_on_read

    if strip_null_bytes_on_write is not NOT_GIVEN:
        if not isinstance(strip_null_bytes_on_write, bool):
            raise TypeError("strip_null_bytes_on_write should be a boolean")

        DEFAULTS.strip_null_bytes_on_write = strip_null_bytes_on_write


@contextmanager
def temporary_defaults(**kwargs):
    try:
        original = DEFAULTS.save()
        set_defaults(**kwargs)
        yield
    finally:
        DEFAULTS.load(original)
