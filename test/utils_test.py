# =============================================================================
# Casanova Utils Unit Tests
# =============================================================================
from casanova.headers import (
    parse_selection,
    SimpleSelection,
    RangeSelection,
    IndexedSelection,
)
from casanova.utils import (
    size_of_row_in_memory,
    size_of_row_in_file,
    CsvCellIO,
    CsvRowIO,
    CsvDictRowIO,
    CsvIO,
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

        assert (
            CsvIO(["name", "surname"], [["John", "Matthews"], ["Lisa", "Orange"]])
            .getvalue()
            .strip()
            == "name,surname\nJohn,Matthews\nLisa,Orange"
        )

    def test_selection_dsl(self):
        fieldnames = [
            "Header1",
            "Header2",
            "Header3",
            "Header4",
            "Foo",
            "Foo",
            "Header5",
            "Foo",
            "Date - Opening",
            "Date - Actual Closing",
            "Header, Whatever",
        ]

        selection = list(
            parse_selection(
                '"Date - Opening","Date - Actual Closing",Header1-Header2,Foo[3],"Header, Whatever",1-4,3-,2,9-5'
            )
        )

        assert selection == [
            SimpleSelection(key="Date - Opening"),
            SimpleSelection(key="Date - Actual Closing"),
            RangeSelection(start="Header1", end="Header2"),
            IndexedSelection(key="Foo", index=3),
            SimpleSelection(key="Header, Whatever"),
            RangeSelection(start=0, end=3),
            RangeSelection(start=2, end=None),
            SimpleSelection(key=2),
            RangeSelection(start=8, end=4),
        ]

        selection = list(parse_selection("!1-4"))

        assert selection == [RangeSelection(start=0, end=3, negative=True)]
