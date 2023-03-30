import platform
from io import StringIO
from contextlib import redirect_stdout

from casanova import Reader
from casanova.__main__ import main

WINDOWS = "windows" in platform.system().lower()


def sort_key(row):
    return tuple(row)


class TestCLI(object):
    def assert_run(self, args, expected, sort=False):
        if WINDOWS:
            return

        output = StringIO(newline="")

        with redirect_stdout(output):
            main(args)

        output.seek(0)
        data = list(Reader(output, no_headers=True))

        if sort:
            data.sort(key=sort_key)
            expected = sorted(expected, key=sort_key)

        assert data == expected

    def test_delimiter(self):
        self.assert_run(
            'map result 42 ./test/resources/people.tsv -d "\t"',
            [
                ["name", "surname", "result"],
                ["Harry", "Golding", "42"],
                ["James", "Henry", "42"],
            ],
        )

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

        self.assert_run(
            "map result 'int(row[headers.n]) * 2' ./test/resources/count.csv",
            [["n", "result"], ["1", "2"], ["2", "4"], ["3", "6"]],
        )

        self.assert_run(
            "map result 'math.floor(math.sqrt(int(row.n) * 10))' ./test/resources/count.csv",
            [["n", "result"], ["1", "3"], ["2", "4"], ["3", "5"]],
        )

    def test_map_init(self):
        self.assert_run(
            "map result -I 's = 34' s ./test/resources/count.csv",
            [["n", "result"], ["1", "34"], ["2", "34"], ["3", "34"]],
        )

    def test_map_before(self):
        self.assert_run(
            "map result -I 's = 10' -B 's += 1' s ./test/resources/count.csv",
            [["n", "result"], ["1", "11"], ["2", "12"], ["3", "13"]],
        )

    def test_map_after(self):
        self.assert_run(
            "map result -I 's = 10' -A 's += 1' s ./test/resources/count.csv",
            [["n", "result"], ["1", "10"], ["2", "11"], ["3", "12"]],
        )

    def test_map_mp(self):
        self.assert_run(
            "map result 42 -p 2 ./test/resources/count.csv",
            [["n", "result"], ["1", "42"], ["2", "42"], ["3", "42"]],
        )

        self.assert_run(
            "map result 42 -p 2 -c 2 ./test/resources/count.csv",
            [["n", "result"], ["1", "42"], ["2", "42"], ["3", "42"]],
        )

        self.assert_run(
            "map result 42 -p 2 -u ./test/resources/count.csv",
            [["n", "result"], ["1", "42"], ["2", "42"], ["3", "42"]],
            sort=True,
        )

    def test_map_module(self):
        self.assert_run(
            "map result -m test.cli_functions ./test/resources/count.csv",
            [["n", "result"], ["1", "10"], ["2", "20"], ["3", "30"]],
        )

        self.assert_run(
            "map result -m test.cli_functions:gen --args '' ./test/resources/count.csv",
            [["n", "result"], ["1", "1|2"], ["2", "1|2"], ["3", "1|2"]],
        )

    def test_map_args(self):
        self.assert_run(
            "map result -m test.cli_functions:enumerate_times_20 --args index ./test/resources/count.csv",
            [["n", "result"], ["1", "0"], ["2", "20"], ["3", "40"]],
        )

    def test_map_formatting(self):
        self.assert_run(
            "map result None ./test/resources/count.csv",
            [["n", "result"], ["1", ""], ["2", ""], ["3", ""]],
        )

        self.assert_run(
            "map result None --none-value none ./test/resources/count.csv",
            [["n", "result"], ["1", "none"], ["2", "none"], ["3", "none"]],
        )

        self.assert_run(
            "map result True --true-value yes ./test/resources/count.csv",
            [["n", "result"], ["1", "yes"], ["2", "yes"], ["3", "yes"]],
        )

        self.assert_run(
            "map result False --false-value no ./test/resources/count.csv",
            [["n", "result"], ["1", "no"], ["2", "no"], ["3", "no"]],
        )

        self.assert_run(
            "map result '[1, 2, 3]' --plural-separator '§' ./test/resources/count.csv",
            [["n", "result"], ["1", "1§2§3"], ["2", "1§2§3"], ["3", "1§2§3"]],
        )

    def test_map_ignore_errors(self):
        self.assert_run(
            "map result ukn ./test/resources/count.csv --ignore-errors",
            [["n", "result"], ["1", ""], ["2", ""], ["3", ""]],
        )

    def test_filter(self):
        self.assert_run(
            'filter "int(row.n) > 2" ./test/resources/count.csv', [["n"], ["3"]]
        )

    def test_reverse(self):
        self.assert_run(
            "reverse ./test/resources/count.csv", [["n"], ["3"], ["2"], ["1"]]
        )
