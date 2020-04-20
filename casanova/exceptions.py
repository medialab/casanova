# =============================================================================
# Casanova Exceptions
# =============================================================================
#


class CasanovaError(Exception):
    pass


class ColumnNumberMismatch(CasanovaError):
    pass


class EmptyFileException(CasanovaError):
    pass


class MissingHeaderException(CasanovaError):
    pass
