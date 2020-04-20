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

            assert reader.pos['name'] == 0
            assert reader.pos['surname'] == 1

            assert reader.pos[0] == 0
            assert reader.pos[1] == 1

            assert len(reader.pos) == 2
            assert reader.fieldnames == ['name', 'surname']

            with pytest.raises(KeyError):
                reader.pos['whatever']

            with pytest.raises(IndexError):
                reader.pos[3]

            surnames = [row[reader.pos.surname] for row in reader]
            assert surnames == ['Matthews', 'Sue', 'Stone']

    def test_no_headers(self):
        with open('./test/resources/no_headers.csv') as f:
            reader = casanova.reader(f, no_headers=True)

            assert reader.fieldnames is None

            surnames = [row[1] for row in reader]
            assert surnames == ['Matthews', 'Sue', 'Stone']
