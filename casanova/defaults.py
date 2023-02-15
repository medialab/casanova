# =============================================================================
# Casanova Global Defaults
# =============================================================================
#
# Global mutable defaults used by casanova classes.
#
DEFAULTS = {
    "prebuffer_bytes": None,
    "strip_null_bytes_on_read": False,
    "strip_null_bytes_on_write": False,
}
NOT_GIVEN = object()


def set_defaults(
    prebuffer_bytes=NOT_GIVEN,
    strip_null_bytes_on_read=NOT_GIVEN,
    strip_null_bytes_on_write=NOT_GIVEN,
):
    global DEFAULTS

    if prebuffer_bytes is not NOT_GIVEN:
        if prebuffer_bytes is not None and (
            not isinstance(prebuffer_bytes, int) or prebuffer_bytes < 1
        ):
            raise TypeError("prebuffer_bytes should be None or a positive integer")

        DEFAULTS["prebuffer_bytes"] = prebuffer_bytes

    if strip_null_bytes_on_read is not NOT_GIVEN:
        if not isinstance(strip_null_bytes_on_read, bool):
            raise TypeError("expecting a boolean")

        DEFAULTS["strip_null_bytes_on_read"] = strip_null_bytes_on_read

    if strip_null_bytes_on_write is not NOT_GIVEN:
        if not isinstance(strip_null_bytes_on_write, bool):
            raise TypeError("expecting a boolean")

        DEFAULTS["strip_null_bytes_on_write"] = strip_null_bytes_on_write
