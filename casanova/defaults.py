# =============================================================================
# Casanova Global Defaults
# =============================================================================
#
# Global mutable defaults used by casanova classes.
#
DEFAULTS = {
    'prebuffer_bytes': None
}


def set_default_prebuffer_bytes(value):
    global DEFAULTS

    if value is not None and (not isinstance(value, int) or value < 1):
        raise TypeError('expecting a positive integer')

    DEFAULTS['prebuffer_bytes'] = value
