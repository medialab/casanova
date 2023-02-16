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
    def __init__(self, msg=None, selection=None):
        super().__init__(msg)
        self.selection = selection
