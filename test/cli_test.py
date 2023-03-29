from io import StringIO
from contextlib import redirect_stdout

from casanova import Reader
from casanova.__main__ import main


class TestCLI(object):
    def assert_run(self, args, expected):
        output = StringIO(newline="")

        with redirect_stdout(output):
            main(args)

        output.seek(0)
        data = list(Reader(output, no_headers=True))

        assert data == expected

    def test_map(self):
        self.assert_run(
            "map result 42 ./test/resources/count.csv",
            [["n", "result"], ["1", "42"], ["2", "42"], ["3", "42"]],
        )

        self.assert_run(
            "map result index ./test/resources/count.csv",
            [["n", "result"], ["1", "0"], ["2", "1"], ["3", "2"]],
        )

        self.assert_run(
            "map result 'int(row.n) * 2' ./test/resources/count.csv",
            [["n", "result"], ["1", "2"], ["2", "4"], ["3", "6"]],
        )
