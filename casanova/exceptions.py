# =============================================================================
# Casanova Exceptions
# =============================================================================
#


class CasanovaError(Exception):
    pass


class ColumnNumberMismatchError(CasanovaError):
    pass


class EmptyFileError(CasanovaError):
    pass


class MissingHeaderError(CasanovaError):
    pass


class InvalidFileError(CasanovaError):
    pass
