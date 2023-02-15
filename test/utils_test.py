# =============================================================================
# Casanova Utils Unit Tests
# =============================================================================
from casanova.utils import (
    size_of_row_in_memory,
    size_of_row_in_file,
    CsvCellIO,
    CsvRowIO,
    CsvDictRowIO,
)


class TestUtils(object):
    def test_size_of_row_in_memory(self):
        assert size_of_row_in_memory([]) == 64
        assert size_of_row_in_memory(["test"]) == 125
        assert size_of_row_in_memory(["hello", "world"]) == 188

    def test_size_of_row_in_file(self):
        assert size_of_row_in_file([]) == 0
        assert size_of_row_in_file(["test"]) == 4
        assert size_of_row_in_file(["hello", "world"]) == 11

    def test_csv_cell_io(self):
        assert CsvCellIO("name", "Yomgui").getvalue().strip() == "name\nYomgui"
        assert (
            CsvCellIO("name", "Yomgui, the real").getvalue().strip()
            == 'name\n"Yomgui, the real"'
        )

    def test_io(self):
        assert (
            CsvRowIO(["tweet_id", "user_id"], ["7274", "971"]).getvalue().strip()
            == "tweet_id,user_id\n7274,971"
        )

        assert (
            CsvDictRowIO({"name": "John", "surname": "Michael"}).getvalue().strip()
            == "name,surname\nJohn,Michael"
        )
