# =============================================================================
# Casanova Exceptions
# =============================================================================
#


class CasanovaException(Exception):
    pass


class ColumnNumberMismatch(CasanovaException):
    pass


class EmptyFileException(CasanovaException):
    pass


class MissingHeaderException(CasanovaException):
    pass
