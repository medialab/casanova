# =============================================================================
# Casanova Reader Unit Tests
# =============================================================================
import gzip
import casanova
import pytest
from io import StringIO

from casanova.reader import DictLikeRow
from casanova.exceptions import (
    EmptyFileError,
    MissingColumnError
)


class TestReader(object):
    def test_exceptions(self):
        with pytest.raises(EmptyFileError):
            casanova.reader(StringIO(''))

        with pytest.raises(TypeError):
            casanova.reader(StringIO('name\nYomgui'), buffer=4.5)

        with pytest.raises(TypeError):
            casanova.reader(StringIO('name\nYomgui'), buffer=-456)

    def test_basics(self):
        with open('./test/resources/people.csv') as f:
            reader = casanova.reader(f)

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

            assert list(reader.pos) == [('name', 0), ('surname', 1)]
            assert dict(list(reader.pos)) == {'name': 0, 'surname': 1}
            assert reader.pos.as_dict() == {'name': 0, 'surname': 1}

            with pytest.raises(KeyError):
                reader.pos['whatever']

            surnames = [row[reader.pos.surname] for row in reader]
            assert surnames == ['Matthews', 'Sue', 'Stone']

    def test_dialect(self):
        with open('./test/resources/semicolons.csv') as f:
            reader = casanova.reader(f, delimiter=';')

            assert [row[0] for row in reader] == ['Rose', 'Luke']

    def test_cells(self):
        with open('./test/resources/people.csv') as f:
            reader = casanova.reader(f)

            with pytest.raises(MissingColumnError):
                reader.cells('whatever')

            names = [name for name in reader.cells('name')]

            assert names == ['John', 'Mary', 'Julia']

        with open('./test/resources/people.csv') as f:
            reader = casanova.reader(f)

            names = [(row[1], name) for row, name in reader.cells('name', with_rows=True)]

            assert names == [('Matthews', 'John'), ('Sue', 'Mary'), ('Stone', 'Julia')]

    def test_records(self):
        with open('./test/resources/people.csv') as f:
            reader = casanova.reader(f)

            with pytest.raises(MissingColumnError):
                reader.cells(['whatever'])

            names = []
            surnames = []

            for name, surname in reader.cells(['name', 'surname']):
                names.append(name)
                surnames.append(surname)

            assert names == ['John', 'Mary', 'Julia']
            assert surnames == ['Matthews', 'Sue', 'Stone']

        with open('./test/resources/people.csv') as f:
            reader = casanova.reader(f)

            names = []
            surnames = []

            for row, (name, surname) in reader.cells(['name', 'surname'], with_rows=True):
                assert len(row) == 2
                names.append(name)
                surnames.append(surname)

            assert names == ['John', 'Mary', 'Julia']
            assert surnames == ['Matthews', 'Sue', 'Stone']

    def test_no_headers(self):
        with open('./test/resources/no_headers.csv') as f:
            reader = casanova.reader(f, no_headers=True)

            assert reader.fieldnames is None

            surnames = [row[1] for row in reader]
            assert surnames == ['Matthews', 'Sue', 'Stone']

    def test_cells_no_headers(self):
        with open('./test/resources/no_headers.csv') as f:
            reader = casanova.reader(f, no_headers=True)

            with pytest.raises(MissingColumnError):
                reader.cells(4)

            names = [name for name in reader.cells(0)]

            assert names == ['John', 'Mary', 'Julia']

    def test_path(self):
        reader = casanova.reader('./test/resources/people.csv')

        assert list(reader.cells('name')) == ['John', 'Mary', 'Julia']

        reader.close()

    def test_context(self):
        with casanova.reader('./test/resources/people.csv') as reader:
            assert list(reader.cells('name')) == ['John', 'Mary', 'Julia']

    def test_invalid_identifier_headers(self):
        with casanova.reader('./test/resources/invalid_headers.csv') as reader:
            assert list(reader.cells('Person\'s name')) == ['John', 'Mary', 'Julia']

    def test_static_count(self):
        count = casanova.reader.count('./test/resources/people.csv')

        assert count == 3

        count = casanova.reader.count('./test/resources/people.csv', max_rows=10)

        assert count == 3

        count = casanova.reader.count('./test/resources/people.csv', max_rows=1)

        assert count is None

        count = casanova.reader.count('./test/resources/people.csv.gz')

        assert count == 3

    def test_gzip(self):
        with gzip.open('./test/resources/people.csv.gz', 'rt') as f:
            reader = casanova.reader(f)

            names = [name for name in reader.cells('name')]

            assert names == ['John', 'Mary', 'Julia']

        with casanova.reader('./test/resources/people.csv.gz') as reader:
            names = [name for name in reader.cells('name')]

            assert names == ['John', 'Mary', 'Julia']

    def test_bom(self):
        with open('./test/resources/bom.csv') as f:
            reader = casanova.reader(f)

            assert reader.fieldnames == ['name', 'color']
            assert 'name' in reader.pos

    def test_wrap(self):
        with open('./test/resources/people.csv') as f:
            reader = casanova.reader(f)

            for row in reader:
                wrapped = reader.wrap(row)

                assert isinstance(wrapped, DictLikeRow)
                assert wrapped['name'] == row[0]
                assert wrapped.surname == row[1]

    def test_prebuffer(self):
        with open('./test/resources/people.csv') as f:
            reader = casanova.reader(f, prebuffer_bytes=1024)

            assert list(reader.cells('surname')) == ['Matthews', 'Sue', 'Stone']
            assert reader.total == 3

        with open('./test/resources/people.csv') as f:
            reader = casanova.reader(f, prebuffer_bytes=2)

            assert list(reader.cells('surname')) == ['Matthews', 'Sue', 'Stone']
            assert reader.total is None

        with open('./test/resources/people.csv') as f:
            reader = casanova.reader(f, prebuffer_bytes=2)

            for surname in reader.cells('surname'):
                assert surname == 'Matthews'
                break

            assert list(reader.cells('surname')) == ['Sue', 'Stone']
