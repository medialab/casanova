# =============================================================================
# Casanova Magic Writer Unit Tests
# =============================================================================
import pytest
from io import StringIO

from test.utils import collect_csv

from casanova.writer import MagicWriter
from casanova.exceptions import InvalidRowTypeError, InconsistentRowTypesError


class TestMagicWriter(object):
    def assert_write(self, data, expected, fieldnames=None):
        output = StringIO()
        writer = MagicWriter(output, fieldnames=fieldnames, none_value="none")
        writer.write(data)

        assert collect_csv(output) == expected

    def test_exceptions(self):
        with pytest.raises(InvalidRowTypeError):
            MagicWriter(StringIO()).write({1, 2, 3})

        with pytest.raises(InconsistentRowTypesError):
            writer = MagicWriter(StringIO())
            writer.write([(1, 2), (3, 4, 5)])

    def test_basics(self):
        # TODO: test with tuples and fieldnames, collections

        self.assert_write("john", [["value"], ["john"]])
        self.assert_write(34, [["value"], ["34"]])
        self.assert_write(True, [["value"], ["true"]])
        self.assert_write(4.5, [["value"], ["4.5"]])
        self.assert_write(None, [["value"], ["none"]])
        self.assert_write({"one": 1, "two": 2}, [["one", "two"], ["1", "2"]])
        self.assert_write([[1, 2, 3]], [["col1", "col2", "col3"], ["1", "2", "3"]])
        self.assert_write([(1, 2, 3)], [["col1", "col2", "col3"], ["1", "2", "3"]])
