# =============================================================================
# Casanova Exceptions
# =============================================================================
#


class CasanovaError(Exception):
    pass


class NoHeadersError(CasanovaError):
    pass


class MissingColumnError(CasanovaError):
    pass


class NotResumableError(CasanovaError):
    pass


class ResumeError(CasanovaError):
    pass


class CorruptedIndexColumnError(CasanovaError):
    pass


class Py310NullByteWriteError(CasanovaError):
    pass


class LtPy311ByteReadError(CasanovaError):
    pass


class InvalidSelectionError(CasanovaError):
    def __init__(self, msg=None, selection=None, reason=None):
        if not msg:
            if isinstance(reason, TypeError):
                msg = str(reason)
            elif isinstance(reason, IndexError):
                msg = "index %s out of range" % str(reason)
            elif isinstance(reason, KeyError):
                msg = "unknown key %s" % str(reason)

        super().__init__(msg or str(reason))
        self.selection = selection
        self.reason = reason
