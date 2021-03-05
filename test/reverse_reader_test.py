# =============================================================================
# Casanova Reverse Reader Unit Tests
# =============================================================================
import casanova
from casanova.exceptions import EmptyFileError
import pytest


class TestReverseReader(object):
    def test_basics(self):
        with open('./test/resources/people.csv') as f:
            reader = casanova.reverse_reader(f)

            names = list(reader.cells('name'))

        assert names == ['Julia', 'Mary', 'John']

    def test_no_headers(self):
        with open('./test/resources/people.csv') as f:
            reader = casanova.reverse_reader(f, no_headers=True)

            names = list(reader.cells(0))

        assert names == ['Julia', 'Mary', 'John', 'name']

    def test_last_cell(self):
        last_cell = casanova.reverse_reader.last_cell('./test/resources/people.csv', 'name')

        assert last_cell == 'Julia'

        last_record = casanova.reverse_reader.last_cell('./test/resources/people.csv', ('surname', 'name'))

        assert last_record == ['Stone', 'Julia']

        last_cell = casanova.reverse_reader.last_cell('./test/resources/people.csv.gz', 'name')

        assert last_cell == 'Julia'

        with pytest.raises(EmptyFileError):
            last_cell = casanova.reverse_reader.last_cell('./test/resources/empty.csv', 'name')

        with pytest.raises(EmptyFileError):
            last_cell = casanova.reverse_reader.last_cell('./test/resources/empty.csv', 'name', no_headers=True)

        with pytest.raises(EmptyFileError):
            last_cell = casanova.reverse_reader.last_cell('./test/resources/empty-with-headers.csv', 'name')
