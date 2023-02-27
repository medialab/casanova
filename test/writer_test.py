# =============================================================================
# Casanova Writer Unit Tests
# =============================================================================
import pytest
from io import StringIO

from test.utils import collect_csv

from casanova.utils import PY_310
from casanova.writer import Writer
from casanova.resumers import LastCellResumer
from casanova.exceptions import Py310NullByteWriteError
from casanova.namedrecord import namedrecord


class TestWriter(object):
    def test_basics(self):
        output = StringIO()
        writer = Writer(output, ["name", "surname"])
        writer.writerow(["John", "Cage"])
        writer.writerow(["Julia", "Andrews"])

        assert collect_csv(output) == [
            ["name", "surname"],
            ["John", "Cage"],
            ["Julia", "Andrews"],
        ]

    def test_resumable(self, tmpdir):
        output_path = str(tmpdir.join("./written_resumable.csv"))

        def stream(offset=0):
            return range(offset, 6)

        with LastCellResumer(output_path, "index") as resumer:
            writer = Writer(resumer, ["index"])

            for i in stream(resumer.get_state() or 0):
                if i == 3:
                    break
                writer.writerow([i])

        assert collect_csv(output_path) == [["index"], ["0"], ["1"], ["2"]]

        with LastCellResumer(output_path, "index") as resumer:
            writer = Writer(resumer, ["index"])

            assert resumer.get_state() == "2"
            n = resumer.pop_state()

            assert n == "2"
            assert resumer.pop_state() is None

            for i in stream(int(n) + 1):
                writer.writerow([i])

        assert collect_csv(output_path) == [
            ["index"],
            ["0"],
            ["1"],
            ["2"],
            ["3"],
            ["4"],
            ["5"],
        ]

    def test_strip_null_bytes_on_write(self):
        output = StringIO()

        writer = Writer(output, fieldnames=["name"], strip_null_bytes_on_write=True)
        writer.writerow(["John\0 Kawazaki"])

        result = output.getvalue().strip()

        assert "\0" not in result

        # Testing non string values
        output = StringIO()

        writer = Writer(output, strip_null_bytes_on_write=True, lineterminator="\n")

        writer.writerow(["John", None, "Davis"])

        assert output.getvalue().strip() == "John,,Davis"

    def test_py310_wrapper(self):
        if not PY_310:
            return

        with pytest.raises(Py310NullByteWriteError):
            writer = Writer(StringIO(), fieldnames=["name"])
            writer.writerow(["John\0 Kawazaki"])

    def test_dialect(self):
        buf = StringIO()

        writer = Writer(
            buf, fieldnames=["name", "surname"], lineterminator="\n", delimiter=";"
        )
        writer.writerow(["John", "Dandy"])

        assert buf.getvalue().strip() == "name;surname\nJohn;Dandy"

    def test_writeheader(self):
        buf = StringIO()

        writer = Writer(
            buf,
            fieldnames=["name", "surname"],
            lineterminator="\n",
            delimiter=";",
            write_header=False,
        )

        writer.writerow(["John", "Dandy"])

        assert buf.getvalue().strip() == "John;Dandy"

    def test_no_headers(self):
        buf = StringIO()

        writer = Writer(
            buf,
            lineterminator="\n",
            delimiter=";",
        )

        writer.writerow(["John", "Dandy"])

        assert buf.getvalue().strip() == "John;Dandy"

    def test_write_namedrecord(self):
        Video = namedrecord("Video", ["title"])

        buf = StringIO()

        writer = Writer(buf, lineterminator="\n")

        writer.writerow(Video("Title"))

        assert buf.getvalue().strip() == "Title"
