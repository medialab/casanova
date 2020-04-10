# =============================================================================
# Casanova Reader Unit Tests
# =============================================================================
import casanova
import pytest
from io import StringIO

from casanova.exceptions import (
    EmptyFileException,
    MissingHeaderException
)


class TestReader(object):
    def test_basics(self):
        with pytest.raises(EmptyFileException):
            casanova.reader(StringIO(''), column='test')

        with open('./test/resources/people.csv') as f:
            with pytest.raises(MissingHeaderException):
                casanova.reader(f, column='notfound')

        with open('./test/resources/people.csv') as f:
            surnames = []

            for surname in casanova.reader(f, column='surname'):
                surnames.append(surname)

            assert surnames == ['Matthews', 'Sue', 'Stone']

    def test_no_headers(self):
        with open('./test/resources/no_headers.csv') as f:
            surnames = []

            for surname in casanova.reader(f, column=1, no_headers=True):
                surnames.append(surname)

            assert surnames == ['Matthews', 'Sue', 'Stone']
