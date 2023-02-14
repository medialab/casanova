# =============================================================================
# Casanova Global Defaults
# =============================================================================
#
# Global mutable defaults used by casanova classes.
#
DEFAULTS = {"prebuffer_bytes": None, "ignore_null_bytes": False}
NOT_GIVEN = object()


def set_defaults(prebuffer_bytes=NOT_GIVEN, ignore_null_bytes=NOT_GIVEN):
    global DEFAULTS

    if prebuffer_bytes is not NOT_GIVEN:
        if prebuffer_bytes is not None and (
            not isinstance(prebuffer_bytes, int) or prebuffer_bytes < 1
        ):
            raise TypeError("prebuffer_bytes should be None or a positive integer")

        DEFAULTS["prebuffer_bytes"] = prebuffer_bytes

    if ignore_null_bytes is not NOT_GIVEN:
        if not isinstance(ignore_null_bytes, bool):
            raise TypeError("expecting a boolean")

        DEFAULTS["ignore_null_bytes"] = ignore_null_bytes
