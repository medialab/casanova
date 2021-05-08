# =============================================================================
# Casanova Utils Unit Tests
# =============================================================================
from casanova.utils import (
    size_of_row_in_memory,
    size_of_row_in_file,
    CsvCellIO
)


class TestUtils(object):
    def test_size_of_row_in_memory(self):
        assert size_of_row_in_memory([]) == 64
        assert size_of_row_in_memory(['test']) == 125
        assert size_of_row_in_memory(['hello', 'world']) == 188

    def test_size_of_row_in_file(self):
        assert size_of_row_in_file([]) == 0
        assert size_of_row_in_file(['test']) == 4
        assert size_of_row_in_file(['hello', 'world']) == 11

    def test_csv_cell_io(self):
        assert CsvCellIO('name', 'Yomgui').getvalue().strip() == 'name\nYomgui'
        assert CsvCellIO('name', 'Yomgui, the real').getvalue().strip() == 'name\n"Yomgui, the real"'
