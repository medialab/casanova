# =============================================================================
# Casanova Reverse Reader Unit Tests
# =============================================================================
import pytest

import casanova
from casanova import Batch
from casanova.exceptions import EmptyFileError


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

    def test_tricky(self):
        with open('./test/resources/tricky_reverse.csv') as f:
            reader = casanova.reader(f)
            rows = list(reader)

        with open('./test/resources/tricky_reverse.csv') as f:
            reverse_reader = casanova.reverse_reader(f)
            rows_read_in_reverse = list(reversed(list(reverse_reader)))

            assert rows_read_in_reverse == rows

    def test_last_cell(self):
        last_cell = casanova.reverse_reader.last_cell('./test/resources/people.csv', 'name')

        assert last_cell == 'Julia'

        last_cell = casanova.reverse_reader.last_cell('./test/resources/people.csv.gz', 'name')

        assert last_cell == 'Julia'

        with pytest.raises(EmptyFileError):
            last_cell = casanova.reverse_reader.last_cell('./test/resources/empty.csv', 'name')

        with pytest.raises(EmptyFileError):
            last_cell = casanova.reverse_reader.last_cell('./test/resources/empty.csv', 'name', no_headers=True)

        with pytest.raises(EmptyFileError):
            last_cell = casanova.reverse_reader.last_cell('./test/resources/empty_with_headers.csv', 'name')

    def test_last_batch(self):
        tests = [
            ('batches', Batch('Jack', finished=True)),
            ('batches_no_end', Batch('Edmund', cursor='cursor1', rows=[['14', 'Edmund', 'Peony', ''], ['13', 'Edmund', 'Carlotta', '']])),
            ('batches_cursor_end', Batch('Edmund', cursor='cursor2')),
            ('batches_broken_first', Batch('Edmund', rows=[['12', 'Edmund', 'Maria', ''], ['11', 'Edmund', 'Maxim', '']])),
            ('batches_raw', Batch('John', rows=[['1', 'John', 'Elisa', ''], ['0', 'John', 'Mary', '']]))
        ]

        for name, expected in tests:
            batch = casanova.reverse_reader.last_batch(
                './test/resources/%s.csv' % name,
                batch_value='name',
                batch_cursor='cursor',
                end_symbol='end'
            )

            assert list(batch) == batch.rows
            assert set(row[2] for row in batch.rows) == batch.collect(2)

            assert batch == expected

        with pytest.raises(EmptyFileError):
            casanova.reverse_reader.last_batch(
                './test/resources/empty.csv',
                batch_value='name',
                batch_cursor='cursor',
                end_symbol='end'
            )

        with pytest.raises(EmptyFileError):
            casanova.reverse_reader.last_batch(
                './test/resources/empty.csv',
                batch_value='name',
                batch_cursor='cursor',
                end_symbol='end'
            )

        with pytest.raises(EmptyFileError):
            casanova.reverse_reader.last_batch(
                './test/resources/empty.csv',
                no_headers=True,
                batch_value='name',
                batch_cursor='cursor',
                end_symbol='end'
            )

        with pytest.raises(EmptyFileError):
            casanova.reverse_reader.last_batch(
                './test/resources/empty_with_headers.csv',
                batch_value='name',
                batch_cursor='surname',
                end_symbol='end'
            )

    def test_ignore_null_bytes(self):
        with open('./test/resources/with_null_bytes.csv') as f:
            reader = casanova.reverse_reader(f, ignore_null_bytes=True)

            assert list(reader) == [
                ['Mary', 'La Croix'],
                ['John', 'Zero']
            ]
