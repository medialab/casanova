# =============================================================================
# Casanova Enricher Unit Tests
# =============================================================================
import casanova
import pytest
from io import StringIO

from casanova.exceptions import (
    EmptyFileException,
    MissingHeaderException
)


class TestEnricher(object):
    def test_basics(self):
        with pytest.raises(EmptyFileException):
            casanova.enricher(StringIO(''), StringIO(''), column='test')

        with open('./test/resources/people.csv') as f:
            with pytest.raises(MissingHeaderException):
                casanova.enricher(f, StringIO(''), column='notfound')
