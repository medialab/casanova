# =============================================================================
# Casanova Inferring Writer Unit Tests
# =============================================================================
import pytest
from io import StringIO
from dataclasses import dataclass

from test.utils import collect_csv

from casanova.writer import InferringWriter
from casanova.record import TabularRecord
from casanova.exceptions import InvalidRowTypeError, InconsistentRowTypesError


class TestInferringWriter(object):
    def assert_writerow(self, data, expected, fieldnames=None):
        output = StringIO()
        writer = InferringWriter(output, fieldnames=fieldnames, none_value="none")
        writer.writerow(data)

        assert collect_csv(output) == expected

    def test_exceptions(self):
        with pytest.raises(InvalidRowTypeError):
            InferringWriter(StringIO()).writerow({1, 2, 3})

        with pytest.raises(InconsistentRowTypesError):
            writer = InferringWriter(StringIO())
            writer.writerow((1, 2))
            writer.writerow((3, 4, 5))

    def test_partial_dict(self):
        output = StringIO()
        writer = InferringWriter(output, none_value="none")

        writer.writerow({"one": 1})
        writer.writerow({"two": 2})

        assert collect_csv(output) == [["one"], ["1"], ["none"]]

    def test_basics(self):
        self.assert_writerow("john", [["value"], ["john"]])
        self.assert_writerow(34, [["value"], ["34"]])
        self.assert_writerow(True, [["value"], ["true"]])
        self.assert_writerow(4.5, [["value"], ["4.5"]])
        self.assert_writerow(None, [["value"], ["none"]])
        self.assert_writerow({"one": 1, "two": 2}, [["one", "two"], ["1", "2"]])
        self.assert_writerow({"one": True}, [["one"], ["true"]])
        self.assert_writerow([1, 2, 3], [["col1", "col2", "col3"], ["1", "2", "3"]])
        self.assert_writerow((1, 2, 3), [["col1", "col2", "col3"], ["1", "2", "3"]])
        self.assert_writerow(range(3), [["col1", "col2", "col3"], ["0", "1", "2"]])
        self.assert_writerow(
            range(3),
            [["one", "two", "three"], ["0", "1", "2"]],
            fieldnames=["one", "two", "three"],
        )
        self.assert_writerow([True, False], [["col1", "col2"], ["true", "false"]])

        @dataclass
        class Document(TabularRecord):
            name: str
            is_relevant: bool

        self.assert_writerow(
            Document("test", False), [["name", "is_relevant"], ["test", "false"]]
        )

        @dataclass
        class RawDocument:
            name: str
            is_relevant: bool

        self.assert_writerow(
            RawDocument("test", False), [["name", "is_relevant"], ["test", "false"]]
        )

    def test_append(self):
        output = StringIO()
        writer = InferringWriter(output, append=["n"])
        writer.writerow("one", [1])
        writer.writerow("two", [2])
        writer.writerow("three", [3])

        assert collect_csv(output) == [
            ["value", "n"],
            ["one", "1"],
            ["two", "2"],
            ["three", "3"],
        ]

        output = StringIO()
        writer = InferringWriter(output, append=["n"], fieldnames=["letters"])
        writer.writerow("one", [1])

        assert collect_csv(output) == [
            ["letters", "n"],
            ["one", "1"],
        ]

    def test_prepend(self):
        output = StringIO()
        writer = InferringWriter(output, prepend=["n"])
        writer.writerow([1], "one")
        writer.writerow([2], "two")
        writer.writerow([3], "three")

        assert collect_csv(output) == [
            ["n", "value"],
            ["1", "one"],
            ["2", "two"],
            ["3", "three"],
        ]

        output = StringIO()
        writer = InferringWriter(output, prepend=["n"], fieldnames=["letters"])
        writer.writerow([1], "one")

        assert collect_csv(output) == [
            ["n", "letters"],
            ["1", "one"],
        ]

    def test_prepend_append(self):
        output = StringIO()
        writer = InferringWriter(output, prepend=["n"], append=["n2"])
        writer.writerow([1], "one", [4])
        writer.writerow([2], "two", [5])
        writer.writerow([3], "three", [6])

        assert collect_csv(output) == [
            ["n", "value", "n2"],
            ["1", "one", "4"],
            ["2", "two", "5"],
            ["3", "three", "6"],
        ]

    def test_buffer_optionals(self):
        output = StringIO()
        writer = InferringWriter(output, buffer_optionals=True)
        writer.writerow(None)
        writer.writerow(None)
        writer.writerow(("John", "Lucy"))
        writer.writerow(None)
        writer.writerow()

        assert collect_csv(output) == [
            ["col1", "col2"],
            ["", ""],
            ["", ""],
            ["John", "Lucy"],
            ["", ""],
            ["", ""],
        ]

        output = StringIO()
        writer = InferringWriter(output, buffer_optionals=True, prepend=["color"])
        writer.writerow(["red"])
        writer.writerow()
        writer.writerow(["yellow"], ("John", "Lucy"))

        assert collect_csv(output) == [
            ["color", "col1", "col2"],
            ["red", "", ""],
            ["", "", ""],
            ["yellow", "John", "Lucy"],
        ]

        output = StringIO()
        writer = InferringWriter(output, buffer_optionals=True)
        writer.writerow()
        writer.writerow()

        del writer

        assert collect_csv(output) == [["col1"], [""], [""]]
