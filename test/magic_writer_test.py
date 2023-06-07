# =============================================================================
# Casanova Magic Writer Unit Tests
# =============================================================================
from io import StringIO

from test.utils import collect_csv

from casanova.writer import MagicWriter


class TestMagicWriter(object):
    def test_basics(self):
        return
        output = StringIO()
        writer = MagicWriter(output)

        writer.write("john")

        assert collect_csv(output) == [["col1"], ["john"]]
