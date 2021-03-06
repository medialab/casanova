# =============================================================================
# Casanova Reader Unit Tests
# =============================================================================
import os
import gzip
import casanova
import pytest
from io import StringIO, BytesIO

from casanova.exceptions import (
    EmptyFileError,
    MissingColumnError
)


def make_reader_test(name, reader_fn, binary=False):
    flag = 'r' if not binary else 'rb'
    gzip_flag = 'rt' if not binary else 'rb'

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

                assert 'name' in reader.pos
                assert 'whatever' not in reader.pos

                assert reader.pos['name'] == 0
                assert reader.pos['surname'] == 1

                assert reader.pos.get('name') == 0
                assert reader.pos.get('whatever') is None
                assert reader.pos.get('whatever', 1) == 1

                assert len(reader.pos) == 2
                assert reader.fieldnames == ['name', 'surname']

                with pytest.raises(KeyError):
                    reader.pos['whatever']

                surnames = [row[reader.pos.surname] for row in reader]
                assert surnames == ['Matthews', 'Sue', 'Stone']

        def test_cells(self):
            with open('./test/resources/people.csv', flag) as f:
                reader = reader_fn(f)

                with pytest.raises(MissingColumnError):
                    reader.cells('whatever')

                names = [name for name in reader.cells('name')]

                assert names == ['John', 'Mary', 'Julia']

            with open('./test/resources/people.csv', flag) as f:
                reader = reader_fn(f)

                names = [(row[1], name) for row, name in reader.cells('name', with_rows=True)]

                assert names == [('Matthews', 'John'), ('Sue', 'Mary'), ('Stone', 'Julia')]

        def test_records(self):
            with open('./test/resources/people.csv', flag) as f:
                reader = reader_fn(f)

                with pytest.raises(MissingColumnError):
                    reader.cells(['whatever'])

                names = []
                surnames = []

                for name, surname in reader.cells(['name', 'surname']):
                    names.append(name)
                    surnames.append(surname)

                assert names == ['John', 'Mary', 'Julia']
                assert surnames == ['Matthews', 'Sue', 'Stone']

            with open('./test/resources/people.csv', flag) as f:
                reader = reader_fn(f)

                names = []
                surnames = []

                for row, (name, surname) in reader.cells(['name', 'surname'], with_rows=True):
                    assert len(row) == 2
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

                with pytest.raises(MissingColumnError):
                    reader.cells(4)

                names = [name for name in reader.cells(0)]

                assert names == ['John', 'Mary', 'Julia']

        def test_path(self):
            reader = reader_fn('./test/resources/people.csv')

            assert list(reader.cells('name')) == ['John', 'Mary', 'Julia']

            reader.close()

        def test_context(self):
            with reader_fn('./test/resources/people.csv') as reader:
                assert list(reader.cells('name')) == ['John', 'Mary', 'Julia']

        def test_invalid_identifier_headers(self):
            with reader_fn('./test/resources/invalid_headers.csv') as reader:
                assert list(reader.cells('Person\'s name')) == ['John', 'Mary', 'Julia']

        def test_static_count(self):
            count = reader_fn.count('./test/resources/people.csv')

            assert count == 3

            count = reader_fn.count('./test/resources/people.csv', max_rows=10)

            assert count == 3

            count = reader_fn.count('./test/resources/people.csv', max_rows=1)

            assert count is None

            count = reader_fn.count('./test/resources/people.csv.gz')

            assert count == 3

        def test_gzip(self):
            with gzip.open('./test/resources/people.csv.gz', mode=gzip_flag) as f:
                reader = reader_fn(f)

                names = [name for name in reader.cells('name')]

                assert names == ['John', 'Mary', 'Julia']

            with reader_fn('./test/resources/people.csv.gz') as reader:
                names = [name for name in reader.cells('name')]

                assert names == ['John', 'Mary', 'Julia']

        def test_bom(self):
            with open('./test/resources/bom.csv', flag) as f:
                reader = reader_fn(f)

                assert reader.fieldnames == ['name', 'color']
                assert 'name' in reader.pos

    return AbstractTestReader


TestReader = make_reader_test('TestReader', casanova.reader)

if not os.environ.get('CASANOVA_TEST_SKIP_CSVMONKEY'):
    import casanova_monkey
    TestMonkeyReader = make_reader_test('TestMonkeyReader', casanova_monkey.reader, binary=True)
