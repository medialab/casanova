# =============================================================================
# Casanova Utils Unit Tests
# =============================================================================
from io import StringIO

from casanova.utils import (
    parse_module_and_target,
    size_of_row_in_memory,
    size_of_row_in_file,
    flatmap,
    CsvCellIO,
    CsvRowIO,
    CsvIO,
    PeekableIterator,
    ReversedFile,
)


class TestUtils(object):
    def test_parse_module_and_target(self):
        assert parse_module_and_target("casanova.module") == (
            "casanova.module",
            "main",
        )
        assert parse_module_and_target("casanova.module:fn") == (
            "casanova.module",
            "fn",
        )

    def test_size_of_row_in_memory(self):
        assert size_of_row_in_memory([]) == 64
        assert size_of_row_in_memory(["test"]) == 125
        assert size_of_row_in_memory(["hello", "world"]) == 188

    def test_size_of_row_in_file(self):
        assert size_of_row_in_file([]) == 0
        assert size_of_row_in_file(["test"]) == 4
        assert size_of_row_in_file(["hello", "world"]) == 11

    def test_flatmap(self):
        assert list(flatmap("test")) == ["test"]
        assert list(flatmap(45)) == [45]
        assert list(flatmap(["one", "two", ["three", [["four", {"five"}]]]])) == [
            "one",
            "two",
            "three",
            "four",
            "five",
        ]

    def test_csv_cell_io(self):
        assert CsvCellIO("Yomgui", "name").getvalue().strip() == "name\nYomgui"
        assert (
            CsvCellIO("Yomgui, the real", "name").getvalue().strip()
            == 'name\n"Yomgui, the real"'
        )

    def test_io(self):
        assert (
            CsvRowIO(["7274", "971"], fieldnames=["tweet_id", "user_id"])
            .getvalue()
            .strip()
            == "tweet_id,user_id\n7274,971"
        )

        assert CsvRowIO(["7274", "971"]).getvalue().strip() == "7274,971"

        assert (
            CsvRowIO({"name": "John", "surname": "Michael"}).getvalue().strip()
            == "name,surname\nJohn,Michael"
        )

        assert (
            CsvRowIO({"name": "John", "surname": "Michael"}, fieldnames=["name", "age"])
            .getvalue()
            .strip()
            == "name,age\nJohn,"
        )

        assert (
            CsvIO(
                [["John", "Matthews"], ["Lisa", "Orange"]],
                fieldnames=["name", "surname"],
            )
            .getvalue()
            .strip()
            == "name,surname\nJohn,Matthews\nLisa,Orange"
        )

        assert (
            CsvIO(
                [["John", "Matthews"], ["Lisa", "Orange"]],
            )
            .getvalue()
            .strip()
            == "John,Matthews\nLisa,Orange"
        )

    def test_peekable_iterator(self):
        it = PeekableIterator([])

        assert it.peek() is None
        assert next(it, None) is None

        it = PeekableIterator(range(1))

        assert it.peek() == 0
        assert next(it) == 0
        assert it.peek() is None
        assert next(it, None) is None
        assert next(it, None) is None

        it = PeekableIterator(range(3))

        assert it.peek() == 0
        assert next(it) == 0
        assert it.peek() == 1
        assert next(it) == 1
        assert it.peek() == 2
        assert next(it) == 2
        assert it.peek() is None
        assert next(it, None) is None
        assert next(it, None) is None
        assert next(it, None) is None

    def test_reversed_file(self):
        f = StringIO("hello world")
        r = ReversedFile(f)

        assert r.read(1) == "d"
        assert r.read(4) == "lrow"
        assert r.read(20) == " olleh"
        assert r.read(5) == ""

        f = StringIO("hello world")
        r = ReversedFile(f, offset=6)

        assert r.read(20) == "dlrow"
