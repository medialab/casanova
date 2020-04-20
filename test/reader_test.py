# =============================================================================
# Casanova Reader Unit Tests
# =============================================================================
import casanova
import pytest
from io import StringIO

from casanova.exceptions import (
    EmptyFileException
)


class TestReader(object):
    def test_exceptions(self):
        with pytest.raises(EmptyFileException):
            casanova.reader(StringIO(''))

    def test_basics(self):
        with open('./test/resources/people.csv') as f:
            reader = casanova.reader(f)

            assert reader.pos.name == 0
            assert reader.pos.surname == 1

            surnames = [row[reader.pos.surname] for row in reader]
            assert surnames == ['Matthews', 'Sue', 'Stone']
