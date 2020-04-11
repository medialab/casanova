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
    def test_exceptions(self):
        with pytest.raises(EmptyFileException):
            casanova.reader(StringIO(''), column='test')

        with open('./test/resources/people.csv') as f:
            with pytest.raises(MissingHeaderException):
                casanova.reader(f, column='notfound')

    def test_basics(self):
        with open('./test/resources/people.csv') as f:
            surnames = []

            for surname in casanova.reader(f, column='surname'):
                surnames.append(surname)

            assert surnames == ['Matthews', 'Sue', 'Stone']

        with open('./test/resources/people.csv') as f:
            surnames = []

            reader = casanova.reader(f, column='surname')

            for row in reader.rows():
                surnames.append(row[reader.pos])

            assert surnames == ['Matthews', 'Sue', 'Stone']

    def test_raw(self):
        with open('./test/resources/no_headers.csv') as f:
            surnames = []

            for row in casanova.reader(f, no_headers=True):
                surnames.append(row[1])

            assert surnames == ['Matthews', 'Sue', 'Stone']

    def test_no_headers(self):
        with open('./test/resources/no_headers.csv') as f:
            surnames = []

            for surname in casanova.reader(f, column=1, no_headers=True):
                surnames.append(surname)

            assert surnames == ['Matthews', 'Sue', 'Stone']

    def test_columns(self):
        with open('./test/resources/people.csv') as f:
            surnames = []
            names = []

            for name, surname in casanova.reader(f, columns=('name', 'surname')):
                surnames.append(surname)
                names.append(name)

            assert surnames == ['Matthews', 'Sue', 'Stone']
            assert names == ['John', 'Mary', 'Julia']

        with open('./test/resources/people.csv') as f:
            surnames = []
            names = []

            for record in casanova.reader(f, columns=('name', 'surname')):
                surnames.append(record.surname)
                names.append(record.name)

                with pytest.raises(AttributeError):
                    record.test

            assert surnames == ['Matthews', 'Sue', 'Stone']
            assert names == ['John', 'Mary', 'Julia']

        with open('./test/resources/people.csv') as f:
            surnames = []
            names = []

            reader = casanova.reader(f, columns=('name', 'surname'))

            for row in reader.rows():
                surnames.append(row[reader.pos.surname])
                names.append(row[reader.pos.name])

                with pytest.raises(IndexError):
                    record[3]

            assert surnames == ['Matthews', 'Sue', 'Stone']
            assert names == ['John', 'Mary', 'Julia']
