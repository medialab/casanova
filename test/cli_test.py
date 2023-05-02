import platform
from io import StringIO
from contextlib import redirect_stdout

from casanova import Reader
from casanova.__main__ import run

WINDOWS = "windows" in platform.system().lower()


def sort_key(row):
    return tuple(row)


class TestCLI(object):
    def assert_run(self, args, expected, sort=False, raw=False):
        if WINDOWS:
            return

        output = StringIO()

        with redirect_stdout(output):
            run(args)

        if raw:
            assert output.getvalue().strip() == expected
            return

        output.seek(0)
        data = list(Reader(output, no_headers=True))

        if sort:
            data.sort(key=sort_key)
            expected = sorted(expected, key=sort_key)

        assert data == expected

    def test_delimiter(self):
        self.assert_run(
            'map 42 result ./test/resources/people.tsv -d "\t"',
            [
                ["name", "surname", "result"],
                ["Harry", "Golding", "42"],
                ["James", "Henry", "42"],
            ],
        )

    def test_map(self):
        self.assert_run(
            "map 42 result ./test/resources/count.csv",
            [["n", "result"], ["1", "42"], ["2", "42"], ["3", "42"]],
        )

        self.assert_run(
            "map index result ./test/resources/count.csv",
            [["n", "result"], ["1", "0"], ["2", "1"], ["3", "2"]],
        )

        self.assert_run(
            "map 'int(row.n) * 2' result ./test/resources/count.csv",
            [["n", "result"], ["1", "2"], ["2", "4"], ["3", "6"]],
        )

        self.assert_run(
            "map 'int(row[headers.n]) * 2' result ./test/resources/count.csv",
            [["n", "result"], ["1", "2"], ["2", "4"], ["3", "6"]],
        )

        self.assert_run(
            "map 'math.floor(math.sqrt(int(row.n) * 10))' result ./test/resources/count.csv",
            [["n", "result"], ["1", "3"], ["2", "4"], ["3", "5"]],
        )

    def test_global_context(self):
        self.assert_run(
            'map "sum(int(row[i]) for i in range(3))" sum ./test/resources/transposed.csv',
            [["one", "two", "three", "sum"], ["35", "23", "26", "84"]],
        )

    def test_map_init(self):
        self.assert_run(
            "map -I 's = 34' s result ./test/resources/count.csv",
            [["n", "result"], ["1", "34"], ["2", "34"], ["3", "34"]],
        )

    def test_map_before(self):
        self.assert_run(
            "map -I 's = 10' -B 's += 1' s result ./test/resources/count.csv",
            [["n", "result"], ["1", "11"], ["2", "12"], ["3", "13"]],
        )

    def test_map_after(self):
        self.assert_run(
            "map -I 's = 10' -A 's += 1' s result ./test/resources/count.csv",
            [["n", "result"], ["1", "10"], ["2", "11"], ["3", "12"]],
        )

    def test_map_mp(self):
        self.assert_run(
            "map 42 -p 2 result ./test/resources/count.csv",
            [["n", "result"], ["1", "42"], ["2", "42"], ["3", "42"]],
        )

        self.assert_run(
            "map 42 -p 2 -c 2 result ./test/resources/count.csv",
            [["n", "result"], ["1", "42"], ["2", "42"], ["3", "42"]],
        )

        self.assert_run(
            "map 42 -p 2 -u result ./test/resources/count.csv",
            [["n", "result"], ["1", "42"], ["2", "42"], ["3", "42"]],
            sort=True,
        )

    def test_map_module(self):
        self.assert_run(
            "map -m test.cli_functions result ./test/resources/count.csv",
            [["n", "result"], ["1", "10"], ["2", "20"], ["3", "30"]],
        )

        self.assert_run(
            "map -m test.cli_functions:gen --args '' result ./test/resources/count.csv",
            [["n", "result"], ["1", "1|2"], ["2", "1|2"], ["3", "1|2"]],
        )

    def test_map_args(self):
        self.assert_run(
            "map -m test.cli_functions:enumerate_times_20 --args index result ./test/resources/count.csv",
            [["n", "result"], ["1", "0"], ["2", "20"], ["3", "40"]],
        )

    def test_map_formatting(self):
        self.assert_run(
            "map None result ./test/resources/count.csv",
            [["n", "result"], ["1", ""], ["2", ""], ["3", ""]],
        )

        self.assert_run(
            "map None --none-value none result ./test/resources/count.csv",
            [["n", "result"], ["1", "none"], ["2", "none"], ["3", "none"]],
        )

        self.assert_run(
            "map True --true-value yes result ./test/resources/count.csv",
            [["n", "result"], ["1", "yes"], ["2", "yes"], ["3", "yes"]],
        )

        self.assert_run(
            "map False --false-value no result ./test/resources/count.csv",
            [["n", "result"], ["1", "no"], ["2", "no"], ["3", "no"]],
        )

        self.assert_run(
            "map '[1, 2, 3]' --plural-separator '§' result ./test/resources/count.csv",
            [["n", "result"], ["1", "1§2§3"], ["2", "1§2§3"], ["3", "1§2§3"]],
        )

    def test_map_ignore_errors(self):
        self.assert_run(
            "map ukn result ./test/resources/count.csv --ignore-errors",
            [["n", "result"], ["1", ""], ["2", ""], ["3", ""]],
        )

    def test_map_select(self):
        self.assert_run(
            'map "int(cell) + 5" result ./test/resources/count.csv -s n',
            [["n", "result"], ["1", "6"], ["2", "7"], ["3", "8"]],
        )

        self.assert_run(
            "map -m test.cli_functions:plus_5 result ./test/resources/count.csv -s n --args cell",
            [["n", "result"], ["1", "6"], ["2", "7"], ["3", "8"]],
        )

        self.assert_run(
            'map -B "name, surname = cells" "name + \'%\' + surname" result ./test/resources/people.csv -s name,surname',
            [
                ["name", "surname", "result"],
                ["John", "Matthews", "John%Matthews"],
                ["Mary", "Sue", "Mary%Sue"],
                ["Julia", "Stone", "Julia%Stone"],
            ],
        )

        self.assert_run(
            "map -m test.cli_functions:concat_name result ./test/resources/people.csv -s name,surname --args cells",
            [
                ["name", "surname", "result"],
                ["John", "Matthews", "John%Matthews"],
                ["Mary", "Sue", "Mary%Sue"],
                ["Julia", "Stone", "Julia%Stone"],
            ],
        )

    def test_flatmap(self):
        self.assert_run(
            'flatmap -B "n = int(row.n)" "[n * 2, n * 3]" result ./test/resources/count.csv',
            [
                ["n", "result"],
                ["1", "2"],
                ["1", "3"],
                ["2", "4"],
                ["2", "6"],
                ["3", "6"],
                ["3", "9"],
            ],
        )

    def test_filter(self):
        self.assert_run(
            'filter "int(row.n) > 2" ./test/resources/count.csv', [["n"], ["3"]]
        )

    def test_map_reduce(self):
        self.assert_run(
            "map-reduce 'int(row.n)' 'acc + current' ./test/resources/count.csv",
            "6",
            raw=True,
        )

        self.assert_run(
            "map-reduce 'int(row.n)' 'acc + current' ./test/resources/count.csv --init-value '-6'",
            "0",
            raw=True,
        )

        self.assert_run(
            "map-reduce -m test.cli_functions test.cli_functions:accumulate ./test/resources/count.csv",
            "6000",
            raw=True,
        )

        self.assert_run(
            """map-reduce -V '{"result": 0}' 'int(row.n)' '{"result": acc["result"] + current}' ./test/resources/count.csv --json""",
            '{"result": 6}',
            raw=True,
        )

        self.assert_run(
            """map-reduce -V '{"result": 0}' 'int(row.n)' '{"result": acc["result"] + current}' ./test/resources/count.csv --json --pretty""",
            '{\n  "result": 6\n}',
            raw=True,
        )

        self.assert_run(
            "map-reduce 'int(row.n)' 'acc + current' ./test/resources/count.csv --csv",
            [["value"], ["6"]],
        )

        self.assert_run(
            "map-reduce 'int(row.n)' 'acc + current' ./test/resources/count.csv --csv -f sum",
            [["sum"], ["6"]],
        )

        self.assert_run(
            """map-reduce -V '{"result": 0}' 'int(row.n)' '{"result": acc["result"] + current, "hello": True}' ./test/resources/count.csv --csv""",
            [["result", "hello"], ["6", "true"]],
        )

        self.assert_run(
            """map-reduce -V '[0, 0]' 'int(row.n)' '[acc[0] + current, acc[1] + 1]' ./test/resources/count.csv --csv""",
            [["col1", "col2"], ["6", "3"]],
        )

        self.assert_run(
            """map-reduce -V '[0, 0]' 'int(row.n)' '[acc[0] + current, acc[1] + 1]' ./test/resources/count.csv --csv -f sum,sum+1""",
            [["sum", "sum+1"], ["6", "3"]],
        )

    def test_groupby(self):
        self.assert_run(
            """groupby 'int(row.n) > 1' 'len(group)' ./test/resources/count.csv""",
            [["group", "value"], ["false", "1"], ["true", "2"]],
        )

        self.assert_run(
            """groupby 'int(row.n) > 1' 'len(group)' ./test/resources/count.csv -f len""",
            [["group", "len"], ["false", "1"], ["true", "2"]],
        )

        self.assert_run(
            """groupby 'int(row.n) > 1' 'len(group), len(group) * 2' ./test/resources/count.csv""",
            [["group", "col1", "col2"], ["false", "1", "2"], ["true", "2", "4"]],
        )

        self.assert_run(
            """groupby 'int(row.n) > 1' 'len(group), len(group) * 2' ./test/resources/count.csv -f len,len2""",
            [["group", "len", "len2"], ["false", "1", "2"], ["true", "2", "4"]],
        )

        self.assert_run(
            """groupby 'int(row.n) > 1' '{"one": len(group), "two": len(group) * 2}' ./test/resources/count.csv""",
            [["group", "one", "two"], ["false", "1", "2"], ["true", "2", "4"]],
        )

        self.assert_run(
            """groupby -m test.cli_functions:grouper test.cli_functions:aggregate ./test/resources/count.csv""",
            [["group", "value"], ["false", "1"], ["true", "2"]],
        )

        self.assert_run(
            """groupby 'None' 'mean(int(row.n) for row in group)' ./test/resources/count.csv --none-value all""",
            [["group", "value"], ["all", "2"]],
        )

    def test_reverse(self):
        self.assert_run(
            "reverse ./test/resources/count.csv", [["n"], ["3"], ["2"], ["1"]]
        )
