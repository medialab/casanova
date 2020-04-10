# =============================================================================
# Casanova Exceptions
# =============================================================================
#


class CasanovaException(Exception):
    pass


class EmptyFileException(CasanovaException):
    pass


class MissingHeaderException(CasanovaException):
    pass
