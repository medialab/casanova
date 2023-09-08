# Casanova as a command line tool

```
usage: casanova [-h] [-d DELIMITER] [-o OUTPUT]
                {map,flatmap,filter,map-reduce,groupby} ...

Casanova command line tool that can be used to mangle CSV files using python
expressions.

available commands:

    - (map): evaluate a python expression for each row of a CSV file
        and save the result as a new column.

    - (flatmap): same as "map" but will iterate over an iterable
        returned by the python expression to output one row per
        yielded item.

    - (filter): evaluate a python expression for each row of a CSV
        file and keep it only if expression returns a truthy value.

    - (map-reduce): evaluate a python expression for each
        row of a CSV file then aggregate the result using
        another python expression.

    - (groupby): group each row of a CSV file using a python
        expression then output some aggregated information
        per group using another python expression.

To perform more generic tasks on CSV files that don't specifically
require executing python code, we recommend using the excellent
and very performant "xsv" tool instead:

https://github.com/BurntSushi/xsv

or our own fork of the tool:

https://github.com/medialab/xsv

positional arguments:
  {map,flatmap,filter,map-reduce,groupby}
                                Command to execute.

optional arguments:
  -d DELIMITER, --delimiter DELIMITER
                                CSV delimiter to use. Defaults to ",".
  -h, --help                    show this help message and exit
  -o OUTPUT, --output OUTPUT    Path to the output file. Will default to stdout
                                and will consider `-` as stdout.
```

## Summary

- [map](#map)
- [flatmap](#flatmap)
- [filter](#filter)
- [map-reduce](#map-reduce)
- [groupby](#groupby)

## map

```
usage: casanova map [-h] [-d DELIMITER] [-o OUTPUT] [-p PROCESSES]
                    [-c CHUNK_SIZE] [-u] [-I INIT] [-B BEFORE] [-A AFTER] [-m]
                    [-a ARGS] [-i] [-s SELECT] [-b BASE_DIR]
                    [--plural-separator PLURAL_SEPARATOR]
                    [--none-value NONE_VALUE] [--true-value TRUE_VALUE]
                    [--false-value FALSE_VALUE]
                    code new_column file

The map command evaluates a python expression
for each row of the given CSV file and writes
a CSV file identical to the input but with an
added column containing the result of the beforementioned
expression.

For instance, given the following CSV file:

a,b
1,4
5,2

The following command:

$ casanova map 'int(row.a) + int(row.b)' c

Will produce the following result:

a,b,c
1,4,5
5,2,7

The evaluation of the python expression can easily
be parallelized using the -p/--processes flag.

positional arguments:
  code                          Python code to evaluate for each row of the CSV
                                file.
  new_column                    Name of the new column to create & containing
                                the result of the evaluated code.
  file                          CSV file to map. Can be gzip-compressed, and can
                                also be a URL. Will consider `-` as stdin.

optional arguments:
  --false-value FALSE_VALUE     String used to serialize False values. Defaults
                                to "false".
  --none-value NONE_VALUE       String used to serialize None values. Defaults
                                to an empty string.
  --plural-separator PLURAL_SEPARATOR
                                Character to use to join lists and sets together
                                in a single cell. Defaults to "|". If you need
                                to emit multiple rows instead, consider using
                                flatmap.
  --true-value TRUE_VALUE       String used to serialize True values. Defaults
                                to "true".
  -A AFTER, --after AFTER       Code to execute after each evaluation of code
                                for a row in the CSV file. Useful to update
                                variables after having returned something. Can
                                be given multiple times.
  -a ARGS, --args ARGS          List of arguments to pass to the function when
                                using -m/--module. Defaults to "row".
  -b BASE_DIR, --base-dir BASE_DIR
                                Base directory to be used by the "read"
                                function.
  -B BEFORE, --before BEFORE    Code to execute before each evaluation of code
                                for a row in the CSV file. Useful to update
                                variables before returning something. Can be
                                given multiple times.
  -c CHUNK_SIZE, --chunk-size CHUNK_SIZE
                                Multiprocessing chunk size. Defaults to 1.
  -d DELIMITER, --delimiter DELIMITER
                                CSV delimiter to use. Defaults to ",".
  -h, --help                    show this help message and exit
  -i, --ignore-errors           If set, evaluation raising an error will be
                                considered as returning None instead of raising.
  -I INIT, --init INIT          Code to execute once before starting to iterate
                                over file. Useful to setup global variables used
                                in evaluated code later. Can be given multiple
                                times.
  -m, --module                  If set, given code will be interpreted as a
                                python module to import and a function name
                                taking the current index and row.
  -o OUTPUT, --output OUTPUT    Path to the output file. Will default to stdout
                                and will consider `-` as stdout.
  -p PROCESSES, --processes PROCESSES
                                Number of processes to use. Defaults to 1.
  -s SELECT, --select SELECT    Use to select columns. First selected column
                                value will be forwared as "cell" and selected
                                column values as "cells".
  -u, --unordered               Whether you allow the result to be in arbitrary
                                order when using multiple processes. Defaults to
                                no.

evaluation variables:

    - (fieldnames): List[str] - list of the CSV file fieldnames
        if the file has headers.

    - (headers): Headers - casanova representation of the CSV
        file headers if any.
        https://github.com/medialab/casanova#headers

    - (index): int - zero-based index of the current row.

    - (row): Row - wrapper class representing the current row.
        It behaves like a python list, but can also be indexed
        like a dict where keys are column names or like an object
        where attributes are column names. It can also be indexed using a
        (str, int) tuple when dealing with CSV files having
        duplicate column names.

        Examples: row[1], row.name, row["name"], row[("name", 1)]

        Note that cell values are always strings. So don't forget
        to parse them if you want to process them as numbers,
        for instance.

        Example: int(row.age)

    - (cells): tuple[str, ...] - tuple containing specific cells
        from the current row that were selected using -s/--select.

    - (cell): str - shorthand for "cells[0]".

available libraries and helpers:

    - (Counter): shorthand for collections.Counter.
        https://docs.python.org/fr/3/library/collections.html#collections.Counter

    - (defaultdict): shorthand for collections.defaultdict.
        https://docs.python.org/fr/3/library/collections.html#collections.defaultdict

    - (deque): shorthand for collections.deque.
        https://docs.python.org/fr/3/library/collections.html#collections.deque

    - (join): shorthand for os.path.join.
        https://docs.python.org/3/library/os.path.html#os.path.join

    - (math): python math module.
        https://docs.python.org/3/library/math.html

    - (random): python random module.
        https://docs.python.org/3/library/random.html

    - (re): python re module (regular expressions).
        https://docs.python.org/3/library/re.html

    - (read): helper function taking a file path and returning
        the file's contents as a str or None if the file
        was not found. Note that it will automatically uncompress
        files with a path ending in ".gz". Optionally takes
        a second argument for the encoding (defaults to "utf-8").

    - (stats): python statistics module.
        https://docs.python.org/3/library/statistics.html

    - (urljoin): shorthand for urllib.parse.urljoin.
        https://docs.python.org/3/library/urllib.parse.html#urllib.parse.urljoin

    - (urlsplit): shorthand for urllib.parse.urlsplit.
        https://docs.python.org/3/library/urllib.parse.html#urllib.parse.urlsplit

Examples:

. Concatenating two columns:
    $ casanova map 'row.name + " " + row.surname' full_name file.csv > result.csv

. Computing a cumulative sum:
    $ casanova map -I 's = 0' -B 's += int(row.count)' s cumsum file.csv > result.csv
```

## flatmap

```
usage: casanova flatmap [-h] [-d DELIMITER] [-o OUTPUT] [-p PROCESSES]
                        [-c CHUNK_SIZE] [-u] [-I INIT] [-B BEFORE] [-A AFTER]
                        [-m] [-a ARGS] [-i] [-s SELECT] [-b BASE_DIR]
                        [--plural-separator PLURAL_SEPARATOR]
                        [--none-value NONE_VALUE] [--true-value TRUE_VALUE]
                        [--false-value FALSE_VALUE] [-r REPLACE]
                        code new_column file

The flatmap command evaluates a python expression
for each row of the given CSV file. This expression
is expected to return a python iterable value that
will be consumed to output one CSV row per yielded item,
containing an additional column with said item, or replacing
a column of your choice (using the -r/--replace flag).

For instance, given the following CSV file:

name,colors
John,blue
Mary,yellow|red

The following command:
$ casanova flatmap 'row.colors.split("|")' color -r colors

Will produce the following result:

name,color
John,blue
Mary,yellow
Mary,red

Note that if the python expression returns an empty
iterable (like an empty tuple), no row will be emitted
in the output. This way, flatmap is sometimes used
as a combination of a filter and a map in a single
pass over the file.

The evaluation of the python expression can easily
be parallelized using the -p/--processes flag.

positional arguments:
  code                          Python code to evaluate for each row of the CSV
                                file.
  new_column                    Name of the new column to create & containing
                                the result of the evaluated code.
  file                          CSV file to flatmap. Can be gzip-compressed, and
                                can also be a URL. Will consider `-` as stdin.

optional arguments:
  --false-value FALSE_VALUE     String used to serialize False values. Defaults
                                to "false".
  --none-value NONE_VALUE       String used to serialize None values. Defaults
                                to an empty string.
  --plural-separator PLURAL_SEPARATOR
                                Character to use to join lists and sets together
                                in a single cell. Defaults to "|". If you need
                                to emit multiple rows instead, consider using
                                flatmap.
  --true-value TRUE_VALUE       String used to serialize True values. Defaults
                                to "true".
  -A AFTER, --after AFTER       Code to execute after each evaluation of code
                                for a row in the CSV file. Useful to update
                                variables after having returned something. Can
                                be given multiple times.
  -a ARGS, --args ARGS          List of arguments to pass to the function when
                                using -m/--module. Defaults to "row".
  -b BASE_DIR, --base-dir BASE_DIR
                                Base directory to be used by the "read"
                                function.
  -B BEFORE, --before BEFORE    Code to execute before each evaluation of code
                                for a row in the CSV file. Useful to update
                                variables before returning something. Can be
                                given multiple times.
  -c CHUNK_SIZE, --chunk-size CHUNK_SIZE
                                Multiprocessing chunk size. Defaults to 1.
  -d DELIMITER, --delimiter DELIMITER
                                CSV delimiter to use. Defaults to ",".
  -h, --help                    show this help message and exit
  -i, --ignore-errors           If set, evaluation raising an error will be
                                considered as returning None instead of raising.
  -I INIT, --init INIT          Code to execute once before starting to iterate
                                over file. Useful to setup global variables used
                                in evaluated code later. Can be given multiple
                                times.
  -m, --module                  If set, given code will be interpreted as a
                                python module to import and a function name
                                taking the current index and row.
  -o OUTPUT, --output OUTPUT    Path to the output file. Will default to stdout
                                and will consider `-` as stdout.
  -p PROCESSES, --processes PROCESSES
                                Number of processes to use. Defaults to 1.
  -r REPLACE, --replace REPLACE
                                What column to optionally replace with the item
                                one in the output CSV file.
  -s SELECT, --select SELECT    Use to select columns. First selected column
                                value will be forwared as "cell" and selected
                                column values as "cells".
  -u, --unordered               Whether you allow the result to be in arbitrary
                                order when using multiple processes. Defaults to
                                no.

evaluation variables:

    - (fieldnames): List[str] - list of the CSV file fieldnames
        if the file has headers.

    - (headers): Headers - casanova representation of the CSV
        file headers if any.
        https://github.com/medialab/casanova#headers

    - (index): int - zero-based index of the current row.

    - (row): Row - wrapper class representing the current row.
        It behaves like a python list, but can also be indexed
        like a dict where keys are column names or like an object
        where attributes are column names. It can also be indexed using a
        (str, int) tuple when dealing with CSV files having
        duplicate column names.

        Examples: row[1], row.name, row["name"], row[("name", 1)]

        Note that cell values are always strings. So don't forget
        to parse them if you want to process them as numbers,
        for instance.

        Example: int(row.age)

    - (cells): tuple[str, ...] - tuple containing specific cells
        from the current row that were selected using -s/--select.

    - (cell): str - shorthand for "cells[0]".

available libraries and helpers:

    - (Counter): shorthand for collections.Counter.
        https://docs.python.org/fr/3/library/collections.html#collections.Counter

    - (defaultdict): shorthand for collections.defaultdict.
        https://docs.python.org/fr/3/library/collections.html#collections.defaultdict

    - (deque): shorthand for collections.deque.
        https://docs.python.org/fr/3/library/collections.html#collections.deque

    - (join): shorthand for os.path.join.
        https://docs.python.org/3/library/os.path.html#os.path.join

    - (math): python math module.
        https://docs.python.org/3/library/math.html

    - (random): python random module.
        https://docs.python.org/3/library/random.html

    - (re): python re module (regular expressions).
        https://docs.python.org/3/library/re.html

    - (read): helper function taking a file path and returning
        the file's contents as a str or None if the file
        was not found. Note that it will automatically uncompress
        files with a path ending in ".gz". Optionally takes
        a second argument for the encoding (defaults to "utf-8").

    - (stats): python statistics module.
        https://docs.python.org/3/library/statistics.html

    - (urljoin): shorthand for urllib.parse.urljoin.
        https://docs.python.org/3/library/urllib.parse.html#urllib.parse.urljoin

    - (urlsplit): shorthand for urllib.parse.urlsplit.
        https://docs.python.org/3/library/urllib.parse.html#urllib.parse.urlsplit

Examples:

. Exploding a column:
    $ casanova flatmap 'row.urls.split("|")' url -r urls file.csv > result.csv
```

## filter

```
usage: casanova filter [-h] [-d DELIMITER] [-o OUTPUT] [-p PROCESSES]
                       [-c CHUNK_SIZE] [-u] [-I INIT] [-B BEFORE] [-A AFTER]
                       [-m] [-a ARGS] [-i] [-s SELECT] [-b BASE_DIR] [-v]
                       code file

The filter command evaluates a python expression
for each row of the given CSV file and only write
it to the output if beforementioned expression
returns a truthy value (where bool(value) is True).

For instance, given the following CSV file:

number
4
5
2

The following command:

$ casanova filter 'int(row.number) >= 4'

Will produce the following result:

number
4
5

The evaluation of the python expression can easily
be parallelized using the -p/--processes flag.

positional arguments:
  code                          Python code to evaluate for each row of the CSV
                                file.
  file                          CSV file to filter. Can be gzip-compressed, and
                                can also be a URL. Will consider `-` as stdin.

optional arguments:
  -A AFTER, --after AFTER       Code to execute after each evaluation of code
                                for a row in the CSV file. Useful to update
                                variables after having returned something. Can
                                be given multiple times.
  -a ARGS, --args ARGS          List of arguments to pass to the function when
                                using -m/--module. Defaults to "row".
  -b BASE_DIR, --base-dir BASE_DIR
                                Base directory to be used by the "read"
                                function.
  -B BEFORE, --before BEFORE    Code to execute before each evaluation of code
                                for a row in the CSV file. Useful to update
                                variables before returning something. Can be
                                given multiple times.
  -c CHUNK_SIZE, --chunk-size CHUNK_SIZE
                                Multiprocessing chunk size. Defaults to 1.
  -d DELIMITER, --delimiter DELIMITER
                                CSV delimiter to use. Defaults to ",".
  -h, --help                    show this help message and exit
  -i, --ignore-errors           If set, evaluation raising an error will be
                                considered as returning None instead of raising.
  -I INIT, --init INIT          Code to execute once before starting to iterate
                                over file. Useful to setup global variables used
                                in evaluated code later. Can be given multiple
                                times.
  -m, --module                  If set, given code will be interpreted as a
                                python module to import and a function name
                                taking the current index and row.
  -o OUTPUT, --output OUTPUT    Path to the output file. Will default to stdout
                                and will consider `-` as stdout.
  -p PROCESSES, --processes PROCESSES
                                Number of processes to use. Defaults to 1.
  -s SELECT, --select SELECT    Use to select columns. First selected column
                                value will be forwared as "cell" and selected
                                column values as "cells".
  -u, --unordered               Whether you allow the result to be in arbitrary
                                order when using multiple processes. Defaults to
                                no.
  -v, --invert-match            Reverse the condition used to filter.

evaluation variables:

    - (fieldnames): List[str] - list of the CSV file fieldnames
        if the file has headers.

    - (headers): Headers - casanova representation of the CSV
        file headers if any.
        https://github.com/medialab/casanova#headers

    - (index): int - zero-based index of the current row.

    - (row): Row - wrapper class representing the current row.
        It behaves like a python list, but can also be indexed
        like a dict where keys are column names or like an object
        where attributes are column names. It can also be indexed using a
        (str, int) tuple when dealing with CSV files having
        duplicate column names.

        Examples: row[1], row.name, row["name"], row[("name", 1)]

        Note that cell values are always strings. So don't forget
        to parse them if you want to process them as numbers,
        for instance.

        Example: int(row.age)

    - (cells): tuple[str, ...] - tuple containing specific cells
        from the current row that were selected using -s/--select.

    - (cell): str - shorthand for "cells[0]".

available libraries and helpers:

    - (Counter): shorthand for collections.Counter.
        https://docs.python.org/fr/3/library/collections.html#collections.Counter

    - (defaultdict): shorthand for collections.defaultdict.
        https://docs.python.org/fr/3/library/collections.html#collections.defaultdict

    - (deque): shorthand for collections.deque.
        https://docs.python.org/fr/3/library/collections.html#collections.deque

    - (join): shorthand for os.path.join.
        https://docs.python.org/3/library/os.path.html#os.path.join

    - (math): python math module.
        https://docs.python.org/3/library/math.html

    - (random): python random module.
        https://docs.python.org/3/library/random.html

    - (re): python re module (regular expressions).
        https://docs.python.org/3/library/re.html

    - (read): helper function taking a file path and returning
        the file's contents as a str or None if the file
        was not found. Note that it will automatically uncompress
        files with a path ending in ".gz". Optionally takes
        a second argument for the encoding (defaults to "utf-8").

    - (stats): python statistics module.
        https://docs.python.org/3/library/statistics.html

    - (urljoin): shorthand for urllib.parse.urljoin.
        https://docs.python.org/3/library/urllib.parse.html#urllib.parse.urljoin

    - (urlsplit): shorthand for urllib.parse.urlsplit.
        https://docs.python.org/3/library/urllib.parse.html#urllib.parse.urlsplit

Examples:

. Filtering rows numerically:
    $ casanova filter 'float(row.weight) >= 0.5' file.csv > result.csv
```

## map-reduce

```
usage: casanova map-reduce [-h] [-d DELIMITER] [-o OUTPUT] [-p PROCESSES]
                           [-c CHUNK_SIZE] [-u] [-I INIT] [-B BEFORE] [-A AFTER]
                           [-m] [-a ARGS] [-i] [-s SELECT] [-b BASE_DIR]
                           [--plural-separator PLURAL_SEPARATOR]
                           [--none-value NONE_VALUE] [--true-value TRUE_VALUE]
                           [--false-value FALSE_VALUE] [--json] [--pretty]
                           [--csv] [-V INIT_VALUE] [-f FIELDNAMES]
                           code accumulator file

The map-reduce command first evaluates a python
expression for each row of the given CSV file,
then accumulates a final result by evaluating
another python expression on the results of the first.

The reducing operation works like with any programming
language, i.e. an accumulated value (set to an arbitrary
value using the -V/--init-value flag, or defaulting
to the first value returned by the mapping expression)
is passed to the reducing expression to be combined with
the current mapped value to produce the next value of
the accumulator.

The result will be printed as a single raw value in
the terminal but can also be formatted as CSV or JSON
using the --csv and --json flags respectively.

For instance, given the following CSV file:

number
4
5
2

The following command:

$ casanova map-reduce 'int(row.number)' 'acc * current'

Will produce the following result:

40

The evaluation of the python expression can easily
be parallelized using the -p/--processes flag.

Note that only the map expression will be parallelized,
not the reduce one.

positional arguments:
  code                          Python code to evaluate for each row of the CSV
                                file.
  accumulator                   Python code that will be evaluated to perform
                                the accumulation towards a final value.
  file                          CSV file to map-reduce. Can be gzip-compressed,
                                and can also be a URL. Will consider `-` as
                                stdin.

optional arguments:
  --csv                         Whether to format the output as csv.
  --false-value FALSE_VALUE     String used to serialize False values. Defaults
                                to "false".
  --json                        Whether to format the output as json.
  --none-value NONE_VALUE       String used to serialize None values. Defaults
                                to an empty string.
  --plural-separator PLURAL_SEPARATOR
                                Character to use to join lists and sets together
                                in a single cell. Defaults to "|". If you need
                                to emit multiple rows instead, consider using
                                flatmap.
  --pretty                      Whether to prettify the output, e.g. indent the
                                json file.
  --true-value TRUE_VALUE       String used to serialize True values. Defaults
                                to "true".
  -A AFTER, --after AFTER       Code to execute after each evaluation of code
                                for a row in the CSV file. Useful to update
                                variables after having returned something. Can
                                be given multiple times.
  -a ARGS, --args ARGS          List of arguments to pass to the function when
                                using -m/--module. Defaults to "row".
  -b BASE_DIR, --base-dir BASE_DIR
                                Base directory to be used by the "read"
                                function.
  -B BEFORE, --before BEFORE    Code to execute before each evaluation of code
                                for a row in the CSV file. Useful to update
                                variables before returning something. Can be
                                given multiple times.
  -c CHUNK_SIZE, --chunk-size CHUNK_SIZE
                                Multiprocessing chunk size. Defaults to 1.
  -d DELIMITER, --delimiter DELIMITER
                                CSV delimiter to use. Defaults to ",".
  -f FIELDNAMES, --fieldnames FIELDNAMES
                                Output CSV file fieldnames. Useful when emitting
                                sequences without keys (e.g. lists, tuples
                                etc.).
  -h, --help                    show this help message and exit
  -i, --ignore-errors           If set, evaluation raising an error will be
                                considered as returning None instead of raising.
  -I INIT, --init INIT          Code to execute once before starting to iterate
                                over file. Useful to setup global variables used
                                in evaluated code later. Can be given multiple
                                times.
  -m, --module                  If set, given code will be interpreted as a
                                python module to import and a function name
                                taking the current index and row.
  -o OUTPUT, --output OUTPUT    Path to the output file. Will default to stdout
                                and will consider `-` as stdout.
  -p PROCESSES, --processes PROCESSES
                                Number of processes to use. Defaults to 1.
  -s SELECT, --select SELECT    Use to select columns. First selected column
                                value will be forwared as "cell" and selected
                                column values as "cells".
  -u, --unordered               Whether you allow the result to be in arbitrary
                                order when using multiple processes. Defaults to
                                no.
  -V INIT_VALUE, --init-value INIT_VALUE
                                Python code to evaluate to initialize the
                                accumulator's value. If not given, the initial
                                value will be the first map result.

evaluation variables:

    - (fieldnames): List[str] - list of the CSV file fieldnames
        if the file has headers.

    - (headers): Headers - casanova representation of the CSV
        file headers if any.
        https://github.com/medialab/casanova#headers

    - (index): int - zero-based index of the current row.

    - (row): Row - wrapper class representing the current row.
        It behaves like a python list, but can also be indexed
        like a dict where keys are column names or like an object
        where attributes are column names. It can also be indexed using a
        (str, int) tuple when dealing with CSV files having
        duplicate column names.

        Examples: row[1], row.name, row["name"], row[("name", 1)]

        Note that cell values are always strings. So don't forget
        to parse them if you want to process them as numbers,
        for instance.

        Example: int(row.age)

    - (cells): tuple[str, ...] - tuple containing specific cells
        from the current row that were selected using -s/--select.

    - (cell): str - shorthand for "cells[0]".

reduce evaluation variables:

    - (acc): Any - accumulated value.

    - (current): Any - value for the current row, as
        returned by the mapped python expression.

available libraries and helpers:

    - (Counter): shorthand for collections.Counter.
        https://docs.python.org/fr/3/library/collections.html#collections.Counter

    - (defaultdict): shorthand for collections.defaultdict.
        https://docs.python.org/fr/3/library/collections.html#collections.defaultdict

    - (deque): shorthand for collections.deque.
        https://docs.python.org/fr/3/library/collections.html#collections.deque

    - (join): shorthand for os.path.join.
        https://docs.python.org/3/library/os.path.html#os.path.join

    - (math): python math module.
        https://docs.python.org/3/library/math.html

    - (random): python random module.
        https://docs.python.org/3/library/random.html

    - (re): python re module (regular expressions).
        https://docs.python.org/3/library/re.html

    - (read): helper function taking a file path and returning
        the file's contents as a str or None if the file
        was not found. Note that it will automatically uncompress
        files with a path ending in ".gz". Optionally takes
        a second argument for the encoding (defaults to "utf-8").

    - (stats): python statistics module.
        https://docs.python.org/3/library/statistics.html

    - (urljoin): shorthand for urllib.parse.urljoin.
        https://docs.python.org/3/library/urllib.parse.html#urllib.parse.urljoin

    - (urlsplit): shorthand for urllib.parse.urlsplit.
        https://docs.python.org/3/library/urllib.parse.html#urllib.parse.urlsplit

Examples:

. Computing the product of a column:
    $ casanova map-reduce 'int(row.number)' 'acc * current' file.csv
```

## groupby

```
usage: casanova groupby [-h] [-d DELIMITER] [-o OUTPUT] [-p PROCESSES]
                        [-c CHUNK_SIZE] [-u] [-I INIT] [-B BEFORE] [-A AFTER]
                        [-m] [-a ARGS] [-i] [-s SELECT] [-b BASE_DIR]
                        [--plural-separator PLURAL_SEPARATOR]
                        [--none-value NONE_VALUE] [--true-value TRUE_VALUE]
                        [--false-value FALSE_VALUE] [-f FIELDNAMES]
                        code aggregator file

The groupby command first evaluates a python
expression for each row of the given CSV file.
This first python expression must return a key
that will be used to add the row to a group.
Then the command will evaluate a second python
expression for each of the groups in order to
output a resulting row for each one of them.

Note that this command needs to load the full
CSV file into memory to work.

For instance, given the following CSV file:

name,surname
John,Davis
Mary,Sue
Marcus,Davis

The following command:

$ casanova groupby 'row.surname' 'len(group)' -f count

Will produce the following result:

group,count
Davis,2
Sue,1

The evaluation of the python expression can easily
be parallelized using the -p/--processes flag.

Note that only the grouping expression will be parallelized,
not the one producing a resulting row for each group.

positional arguments:
  code                          Python code to evaluate to group each row of the
                                CSV file.
  aggregator                    Python code that will be evaluated to perform
                                the aggregation of each yielded group of rows.
  file                          CSV file to group. Can be gzip-compressed, and
                                can also be a URL. Will consider `-` as stdin.

optional arguments:
  --false-value FALSE_VALUE     String used to serialize False values. Defaults
                                to "false".
  --none-value NONE_VALUE       String used to serialize None values. Defaults
                                to an empty string.
  --plural-separator PLURAL_SEPARATOR
                                Character to use to join lists and sets together
                                in a single cell. Defaults to "|". If you need
                                to emit multiple rows instead, consider using
                                flatmap.
  --true-value TRUE_VALUE       String used to serialize True values. Defaults
                                to "true".
  -A AFTER, --after AFTER       Code to execute after each evaluation of code
                                for a row in the CSV file. Useful to update
                                variables after having returned something. Can
                                be given multiple times.
  -a ARGS, --args ARGS          List of arguments to pass to the function when
                                using -m/--module. Defaults to "row".
  -b BASE_DIR, --base-dir BASE_DIR
                                Base directory to be used by the "read"
                                function.
  -B BEFORE, --before BEFORE    Code to execute before each evaluation of code
                                for a row in the CSV file. Useful to update
                                variables before returning something. Can be
                                given multiple times.
  -c CHUNK_SIZE, --chunk-size CHUNK_SIZE
                                Multiprocessing chunk size. Defaults to 1.
  -d DELIMITER, --delimiter DELIMITER
                                CSV delimiter to use. Defaults to ",".
  -f FIELDNAMES, --fieldnames FIELDNAMES
                                Output CSV file fieldnames. Useful when emitting
                                sequences without keys (e.g. lists, tuples
                                etc.).
  -h, --help                    show this help message and exit
  -i, --ignore-errors           If set, evaluation raising an error will be
                                considered as returning None instead of raising.
  -I INIT, --init INIT          Code to execute once before starting to iterate
                                over file. Useful to setup global variables used
                                in evaluated code later. Can be given multiple
                                times.
  -m, --module                  If set, given code will be interpreted as a
                                python module to import and a function name
                                taking the current index and row.
  -o OUTPUT, --output OUTPUT    Path to the output file. Will default to stdout
                                and will consider `-` as stdout.
  -p PROCESSES, --processes PROCESSES
                                Number of processes to use. Defaults to 1.
  -s SELECT, --select SELECT    Use to select columns. First selected column
                                value will be forwared as "cell" and selected
                                column values as "cells".
  -u, --unordered               Whether you allow the result to be in arbitrary
                                order when using multiple processes. Defaults to
                                no.

evaluation variables:

    - (fieldnames): List[str] - list of the CSV file fieldnames
        if the file has headers.

    - (headers): Headers - casanova representation of the CSV
        file headers if any.
        https://github.com/medialab/casanova#headers

    - (index): int - zero-based index of the current row.

    - (row): Row - wrapper class representing the current row.
        It behaves like a python list, but can also be indexed
        like a dict where keys are column names or like an object
        where attributes are column names. It can also be indexed using a
        (str, int) tuple when dealing with CSV files having
        duplicate column names.

        Examples: row[1], row.name, row["name"], row[("name", 1)]

        Note that cell values are always strings. So don't forget
        to parse them if you want to process them as numbers,
        for instance.

        Example: int(row.age)

    - (cells): tuple[str, ...] - tuple containing specific cells
        from the current row that were selected using -s/--select.

    - (cell): str - shorthand for "cells[0]".

grouping evaluation variables:

    - (group): Group: wrapper class representing
        a group of CSV rows. You can get its length,
        its key and iterate over its rows.

        Examples:
            len(group)
            group.key
            sum(int(row.count) for row in group)

available libraries and helpers:

    - (Counter): shorthand for collections.Counter.
        https://docs.python.org/fr/3/library/collections.html#collections.Counter

    - (defaultdict): shorthand for collections.defaultdict.
        https://docs.python.org/fr/3/library/collections.html#collections.defaultdict

    - (deque): shorthand for collections.deque.
        https://docs.python.org/fr/3/library/collections.html#collections.deque

    - (join): shorthand for os.path.join.
        https://docs.python.org/3/library/os.path.html#os.path.join

    - (math): python math module.
        https://docs.python.org/3/library/math.html

    - (random): python random module.
        https://docs.python.org/3/library/random.html

    - (re): python re module (regular expressions).
        https://docs.python.org/3/library/re.html

    - (read): helper function taking a file path and returning
        the file's contents as a str or None if the file
        was not found. Note that it will automatically uncompress
        files with a path ending in ".gz". Optionally takes
        a second argument for the encoding (defaults to "utf-8").

    - (stats): python statistics module.
        https://docs.python.org/3/library/statistics.html

    - (urljoin): shorthand for urllib.parse.urljoin.
        https://docs.python.org/3/library/urllib.parse.html#urllib.parse.urljoin

    - (urlsplit): shorthand for urllib.parse.urlsplit.
        https://docs.python.org/3/library/urllib.parse.html#urllib.parse.urlsplit

Examples:

. Computing a mean by group:
    $ casanova groupby 'row.city' 'stats.mean(int(row.count) for row in group)' file.csv > result.csv
```

