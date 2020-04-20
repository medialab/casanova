# =============================================================================
# Casanova Reader Unit Tests
# =============================================================================
import casanova
import casanova_monkey
import pytest
from io import StringIO, BytesIO

from casanova.exceptions import (
    EmptyFileError,
    MissingHeaderError
)


def make_reader_test(name, reader_fn, binary=False):
    flag = 'r' if not binary else 'rb'

    class AbstractTestReader(object):
        __name__ = name

        def test_exceptions(self):
            with pytest.raises(EmptyFileError):
                reader_fn(StringIO('') if not binary else BytesIO(b''))

        def test_basics(self):
            with open('./test/resources/people.csv', flag) as f:
                reader = reader_fn(f)

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

        def test_cells(self):
            with open('./test/resources/people.csv', flag) as f:
                reader = reader_fn(f)

                with pytest.raises(MissingHeaderError):
                    reader.cells('whatever')

                names = [name for name in reader.cells('name')]

                assert names == ['John', 'Mary', 'Julia']

        def test_records(self):
            with open('./test/resources/people.csv', flag) as f:
                reader = reader_fn(f)

                with pytest.raises(MissingHeaderError):
                    reader.records(['whatever'])

                names = []
                surnames = []

                for name, surname in reader.cells(['name', 'surname']):
                    names.append(name)
                    surnames.append(surname)

                assert names == ['John', 'Mary', 'Julia']
                assert surnames == ['Matthews', 'Sue', 'Stone']

        def test_no_headers(self):
            with open('./test/resources/no_headers.csv', flag) as f:
                reader = reader_fn(f, no_headers=True)

                assert reader.fieldnames is None

                surnames = [row[1] for row in reader]
                assert surnames == ['Matthews', 'Sue', 'Stone']

        def test_cells_no_headers(self):
            with open('./test/resources/no_headers.csv', flag) as f:
                reader = reader_fn(f, no_headers=True)

                with pytest.raises(MissingHeaderError):
                    reader.cells(4)

                names = [name for name in reader.cells(0)]

                assert names == ['John', 'Mary', 'Julia']

    return AbstractTestReader


TestReader = make_reader_test('TestReader', casanova.reader)
# TestMonkeyReader = make_reader_test('TestMonkeyReader', casanova_monkey.reader, binary=True)
