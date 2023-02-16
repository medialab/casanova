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
        "plural_separator",
        "none_value",
        "true_value",
        "false_value",
        "ignore_false",
    ]

    def __init__(self):
        self.prebuffer_bytes = None
        self.strip_null_bytes_on_read = False
        self.strip_null_bytes_on_write = False
        self.plural_separator = "|"
        self.none_value = ""
        self.true_value = "true"
        self.false_value = "false"
        self.ignore_false = False

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
    ignore_false=NOT_GIVEN,
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

    if plural_separator is not NOT_GIVEN:
        if not isinstance(plural_separator, str):
            raise TypeError("plural_separator should be a string")

        DEFAULTS.plural_separator = plural_separator

    if none_value is not NOT_GIVEN:
        if not isinstance(none_value, str):
            raise TypeError("none_value should be a string")

        DEFAULTS.none_value = none_value

    if true_value is not NOT_GIVEN:
        if not isinstance(true_value, str):
            raise TypeError("true_value should be a string")

        DEFAULTS.true_value = true_value

    if false_value is not NOT_GIVEN:
        if not isinstance(false_value, str):
            raise TypeError("false_value should be a string")

        DEFAULTS.false_value = false_value

    if ignore_false is not NOT_GIVEN:
        if not isinstance(ignore_false, bool):
            raise TypeError("ignore_false should be a boolean")

        DEFAULTS.ignore_false = false_value


@contextmanager
def temporary_defaults(**kwargs):
    try:
        original = DEFAULTS.save()
        set_defaults(**kwargs)
        yield
    finally:
        DEFAULTS.load(original)
