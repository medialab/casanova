# =============================================================================
# Casanova Inferring Writer Unit Tests
# =============================================================================
import pytest
from io import StringIO
from dataclasses import dataclass

from test.utils import collect_csv

from casanova.writer import InferringWriter
from casanova.namedrecord import namedrecord, TabularRecord
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

        Video = namedrecord("Video", ["title", "duration"])

        self.assert_writerow(
            Video("Jaws", 145), [["title", "duration"], ["Jaws", "145"]]
        )

        @dataclass
        class Document(TabularRecord):
            name: str
            is_relevant: bool

        self.assert_writerow(
            Document("test", False), [["name", "is_relevant"], ["test", "false"]]
        )
