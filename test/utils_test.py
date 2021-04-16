# =============================================================================
# Casanova Utils Unit Tests
# =============================================================================
from casanova.utils import count_bytes_in_row, CsvCellIO


class TestUtils(object):
    def test_count_bytes_in_row(self):
        assert count_bytes_in_row([]) == 0
        assert count_bytes_in_row(['test']) == 8
        assert count_bytes_in_row(['hello', 'world']) == 20

    def test_csv_cell_io(self):
        assert CsvCellIO('name', 'Yomgui').getvalue().strip() == 'name\nYomgui'
        assert CsvCellIO('name', 'Yomgui, the real').getvalue().strip() == 'name\n"Yomgui, the real"'
