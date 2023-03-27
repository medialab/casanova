[![Build Status](https://github.com/medialab/casanova/workflows/Tests/badge.svg)](https://github.com/medialab/casanova/actions)

# casanova

If you often find yourself processing CSV files using python, you will quickly notice that, while being more comfortable, `csv.DictReader` remains way slower than `csv.reader`:

```
# To read a 1.5G CSV file:
csv.reader: 24s
csv.DictReader: 84s
casanova.reader: 25s
```

`casanova` is therefore an attempt to stick to `csv.reader` performance while still keeping a comfortable interface, still able to consider headers (even duplicate ones also, something that `csv.DictReader` is incapable of) etc.

`casanova` is thus a good fit for you if you need to:

- Stream large CSV files without running out of memory
- Enrich the same CSV files by outputing a similar file, all while adding, filtering and editing cells.
- Have the possibility to resume said enrichment if your process exited
- Do so in a threadsafe fashion, and be able to resume even if your output does not have the same order as the input

`casanova` also packs exotic utilities able to read csv files in reverse (without loading the whole file into memory and in regular `O(n)` time), so you can, for instance, fetch useful information to restart some aborted process.

## Installation

You can install `casanova` with pip with the following command:

```
pip install casanova
```

If you want to be able to feed CSV files from the web to `casanova` readers & enrichers you will also need to install at least `urllib3` and optionally `certifi` (if you want secure SSL). Nnote that a lot of python packages, including the popular `requests` library, already depend on those two, so it is likely you already have them installed anyway:

```
# Installing them explicitly
pip install urllib3 certifi

# Installing casanova with those implicitly
pip install casanova[http]
```

## Usage

- [reader](#reader)
- [reverse_reader](#reverse_reader)
- [headers](#headers)
- [writer](#writer)
- [enricher](#enricher)
- [threadsafe_enricher](#threadsafe_enricher)
- [batch_enricher](#batch_enricher)
- [resumers](#resumers)
  - [RowCountResumer](#rowcountresumer)
  - [ThreadSafeResumer](#threadsaferesumer)
  - [BatchResumer](#batchresumer)
  - [LastCellResumer](#lastcellresumer)
  - [LastCellComparisonResumer](#lastcellcomparisonresumer)
- [count](#count)
- [last_cell](#last_cell)
- [namedrecord](#namedrecord)
- [set_defaults](#set_defaults)
- [xsv selection mini DSL](#xsv-selection-mini-dsl)

## reader

Straightforward CSV reader yielding list rows but giving some information about potential headers and their ipositions.

```python
import casanova

with open('./people.csv') as f:

    # Creating a reader
    reader = casanova.reader(f)

    # Getting header information
    reader.fieldnames
    >>> ['name', 'surname']

    reader.headers
    >>> Headers(name=0, surname=1)

    name_pos = reader.headers.name
    name_pos = reader.headers['name']

    'name' in reader.headers
    >>> True

    # Iterating over the rows
    for row in reader:
        name = row[name_pos] # it's better to cache your pos outside the loop
        name = row[reader.headers.name] # this works, but is slower

    # Interested in a single column?
    for name in reader.cells('name'):
        print(name)

    # Need also the current row when iterating on cells?
    for row, name in reader.cells('name', with_rows=True):
        print(row, name, surname)

    # Want to iterate over records
    # NOTE: this has a performance cost
    for name, surname in reader.records('name', 'surname'):
        print(name, surname)

    for record in reader.records(['name', 'age']):
        print(record[0])

    for record in reader.records({'name': 'name', 'age': 1}):
        print(record['age'])

    # No headers? No problem.
    reader = casanova.reader(f, no_headers=True)

# Note that you can also create a reader from a path
with casanova.reader('./people.csv') as reader:
  ...

# And if you need exotic encodings
with casanova.reader('./people.csv', encoding='latin1') as reader:
  ...

# The reader will also handle gzipped files out of the box
with casanova.reader('./people.csv.gz') as reader:
  ...

# If you have `urllib3` installed, casanova is also able to stream
# remote CSV file out of the box
with casanova.reader('https://mydomain.fr/some-file.csv') as reader:
    ...

# The reader will also accept iterables of rows
rows = [['name', 'surname'], ['John', 'Moran']]
reader = casanova.reader(rows)

# And you can of course use the typical dialect-related kwargs
reader = casanova.reader('./french-semicolons.csv', delimiter=';')

# Readers can also be closed if you want to avoid context managers
reader.close()
```

_Arguments_

- **input_file** _str or Path or file or Iterable[list[str]]_: input file given to the reader. Can be a path that will be opened for you by the reader, a file handle or even an arbitrary iterable of list rows.
- **no_headers** _bool, optional_ [`False`]: set to `True` if `input_file` has no headers.
- **encoding** _str, optional_ [`utf-8`]: encoding to use to open the file if `input_file` is a path.
- **dialect** _str or csv.Dialect, optional_: CSV dialect for the reader to use. Check python standard [csv](https://docs.python.org/3/library/csv.html) module documentation for more info.
- **quotechar** _str, optional_: quote character used by CSV parser.
- **delimiter** _str, optional_: delimiter characted used by CSV parser.
- **prebuffer_bytes** _int, optional_: number of bytes of input file to prebuffer in attempt to get a total number of lines ahead of time.
- **total** _int, optional_: total number of lines to expect in file, if you already know it ahead of time. If given, the reader won't prebuffer data even if `prebuffer_bytes` was set.
- **multiplex** _casanova.Multiplexer, optional_: multiplexer to use. Read [this](#multiplexing) for more information.
- **strip_null_bytes_on_read** _bool, optional_ [`False`]: before python 3.11, the `csv` module will raise when attempting to read a CSV file containing null bytes. If set to `True`, the reader will strip null bytes on the fly while parsing rows.
- **reverse** _bool, optional_ [`False`]: whether to read the file in reverse (except for the header of course).

_Properties_

- **total** _int, optional_: total number of lines in the file, if known through prebuffering or through the `total` kwarg.
- **headers** _casanova.Headers, optional_, optional: CSV file headers if `no_headers=False`.
- **empty** _bool_: whether the given file was empty.
- **fieldnames** _list[str], optional_: list representing the CSV file headers if `no_headers=False`.
- **row_len** _int_: expected number of items per row.

_Methods_

- **rows**: returns an iterator over the reader rows. Same as iterating over the reader directly.
- **cells**: take the name of a column or its position and returns an iterator over values of the given column. Can be given `with_rows=True` if you want to iterate over a `value, row` tuple instead if required.
- **enumerate**: resuming-safe enumeration over rows yielding `index, row` tuples. Takes an optional `start` kwarg like builtin `enumerate`.
- **enumerate_cells**: resuming-safe enumeration over cells yielding `index, cell` or `index, row, cell` if given `with_rows=True`. Takes an optional `start` kwarg like builtin `enumerate`.
- **wrap**: method taking a list row and returning a `RowWrapper` object to wrap it.
- **close**: cleans up the reader resources manually when not using the dedicated context manager. It is usually only useful when the reader was given a path and not an already opened file handle.

<em id="multiplexing">Multiplexing</em>

Sometimes, one column of your CSV file might contain multiple values, separated by an arbitrary separator character such as `|`.

In this case, it might be desirable to "multiplex" the file by making a reader emit one copy of the line with each of the values contained by a cell.

To do so, `casanova` exposes a special `Multiplexer` object you can give to any reader like so:

```python
import casanova

# Most simple case: a column named "colors", separated by "|"
reader = casanova.reader(
    input_file,
    multiplex=casanova.Multiplexer('colors')
)

# Customizing the separator:
reader = casanova.reader(
    input_file,
    multiplex=casanova.Multiplexer('colors', separator='$')
)

# Renaming the column on the fly:
reader = casanova.reader(
    input_file,
    multiplex=casanova.Multiplexer('colors', new_column='color')
)
```

## reverse_reader

A reverse CSV reader might sound silly, but it can be useful in some scenarios. Especially when you need to read the last line from an output file without reading the whole thing first, in constant time.

It is mostly used by `casanova` [resumers](#resumers) and it is unlikely you will need to use them on your own.

```python
import casanova

# people.csv looks like this
# name,surname
# John,Doe,
# Mary,Albert
# Quentin,Gold

with open('./people.csv', 'rb') as f:
    reader = casanova.reverse_reader(f)

    reader.fieldnames
    >>> ['name', 'surname']

    next(reader)
    >>> ['Quentin', 'Gold']
```

## headers

A class representing the headers of a CSV file. It is useful to find the row position of some columns and perform complex selection.

```python
import casanova

# Headers can be instantiated thusly
headers = casanova.headers(['name', 'surname', 'age'])

# But you will usually use a reader or an enricher's one:
headers = casanova.reader(input_file).headers

# Accessing a column through attributes
headers.surname
>>> 1

# Accessing a column by indexing:
headers['surname']
>>> 1

# Getting a column
headers.get('surname')
>>> 1
headers.get('not-found')
>>> None

# Getting a duplicated column name
casanova.headers(['surname', 'name', 'name'])['name', 1]
>>> 2

casanova.headers(['surname', 'name', 'name']).get('name', index=1)
>>> 2

# Asking if a column exists:
'name' in headers:
>>> True

# Retrieving fieldnames:
headers.fieldnames
>>> ['name', 'surname', 'age']

# Iterating over headers
for col in headers:
    print(col)

# Couting columns:
len(headers)
>>> 3

# Retrieving the nth header:
headers.nth(1)
>>> 'surname'

# Wraping a row
headers.wrap(['John', 'Matthews', '45'])
>>> RowWrapper(name='John', surname='Matthews', age='45')

# Selecting some columns (by name and/or index)):
headers.select(['name', 2])
>>> [0, 2]

# Selecting using xsv mini DSL:
headers.select('name,age')
>>> [0, 2]
headers.select('!name')
>>> [1, 2]
```

For more info about xsv mini DSL, check [this](#xsv-selection-mini-dsl) part of the documentation.

## writer

`casanova` also exports a csv writer. It can automatically write headers when needed and is able to resume some tasks.

```python
import casanova

with open('output.csv') as f:
    writer = casanova.writer(f, fieldnames=['name', 'surname'])

    writer.writerow(['John', 'Davis'])

    # If you want to write headers yourself:
    writer = casanova.writer(f, fieldnames=['name', 'surname'], write_header=False)

    writer.writeheader()
```

_Arguments_

- **output_file** _file or casanova.Resumer_: target file.
- **fieldnames** _Iterable[str], optional_: column names.
- **strip_null_bytes_on_write** _bool, optional_ [`False`]: whether to strip null bytes when writing rows. Note that on python 3.10, there is a bug that prevents a `csv.writer` will raise an error when attempting to write a row containing a null byte.
- **dialect** _csv.Dialect or str, optional_: dialect to use to write CSV.
- **delimiter** _str, optional_: CSV delimiter.
- **quotechar** _str, optional_: CSV quoting character.
- **quoting** _csv.QUOTE\_\*, optional_: CSV quoting strategy.
- **escapechar** _str, optional_: CSV escaping character.
- **lineterminator** _str, optional_: CSV line terminator.
- **write_header** _bool, optional_ [`True`]: whether to automatically write header if required (takes resuming into account).

_Properties_

- **headers** _casanova.Headers, optional_, optional: CSV file headers if fieldnames were provided
- **fieldnames** _list[str], optional_: provided fieldnames.

_Resuming_

A `casanova.writer` is able to resume through a [`LastCellResumer`](#lastcellresumer).

## enricher

`casanova` enrichers are basically a smart combination of both a reader and a writer.

It can be used to transform a given CSV file. This means you can transform its values on the fly, select some columns to keep from input and add new ones very easily.

Note that enrichers inherits from both [`casanova.reader`](#reader) and [`casanova.writer`](#writer) and therefore keep both their properties and methods.

```python
import casanova

with open('./people.csv') as input_file, \
     open('./enriched-people.csv', 'w') as output_file:

    enricher = casanova.enricher(input_file, output_file)

    # The enricher inherits from casanova.reader
    enricher.fieldnames
    >>> ['name', 'surname']

    # You can iterate over its rows
    name_pos = enricher.headers.name

    for row in enricher:

        # Editing a cell, so that everyone is called John now
        row[name_pos] = 'John'
        enricher.writerow(row)

    # Want to add columns?
    enricher = casanova.enricher(f, of, add=['age', 'hair'])

    for row in enricher:
        enricher.writerow(row, ['34', 'blond'])

    # Want to keep only some columns from input?
    enricher = casanova.enricher(f, of, add=['age'], select=['surname'])

    for row in enricher:
        enricher.writerow(row, ['45'])

    # Want to select columns to keep using xsv mini dsl?
    enricher = casanova.enricher(f, of, select='!1-4')

    # You can of course still use #.cells etc.
    for row, name in enricher.cells('name', with_rows=True):
        print(row, name)
```

_Arguments_

- **input_file** _file or str_: file object to read or path to open.
- **output_file** _file or Resumer_: file object to write.
- **no_headers** _bool, optional_ [`False`]: set to `True` if `input_file` has no headers.
- **encoding** _str, optional_ [`utf-8`]: encoding to use to open the file if `input_file` is a path.
- **add** _Iterable[str|int], optional_: names of columns to add to output.
- **select** _Iterable[str|int]|str, optional_: selection of columns to keep from input. Can be an iterable of column names and/or column positions or a selection string writting in [xsv mini DSL](#xsv-selection-mini-dsl).
- **dialect** _str or csv.Dialect, optional_: CSV dialect for the reader to use. Check python standard [csv](https://docs.python.org/3/library/csv.html) module documentation for more info.
- **quotechar** _str, optional_: quote character used by CSV parser.
- **delimiter** _str, optional_: delimiter characted used by CSV parser.
- **prebuffer_bytes** _int, optional_: number of bytes of input file to prebuffer in attempt to get a total number of lines ahead of time.
- **total** _int, optional_: total number of lines to expect in file, if you already know it ahead of time. If given, the reader won't prebuffer data even if `prebuffer_bytes` was set.
- **multiplex** _casanova.Multiplexer, optional_: multiplexer to use. Read [this](#multiplexing) for more information.
- **reverse** _bool, optional_ [`False`]: whether to read the file in reverse (except for the header of course).
- **strip_null_bytes_on_read** _bool, optional_ [`False`]: before python 3.11, the `csv` module will raise when attempting to read a CSV file containing null bytes. If set to `True`, the reader will strip null bytes on the fly while parsing rows.
- **strip_null_bytes_on_write** _bool, optional_ [`False`]: whether to strip null bytes when writing rows. Note that on python 3.10, there is a bug that prevents a `csv.writer` will raise an error when attempting to write a row containing a null byte.
- **writer_dialect** _csv.Dialect or str, optional_: dialect to use to write CSV.
- **writer_delimiter** _str, optional_: CSV delimiter for writer.
- **writer_quotechar** _str, optional_: CSV quoting character for writer.
- **writer_quoting** _csv.QUOTE\_\*, optional_: CSV quoting strategy for writer.
- **writer_escapechar** _str, optional_: CSV escaping character for writer.
- **writer_lineterminator** _str, optional_: CSV line terminator for writer.
- **write_header** _bool, optional_ [`True`]: whether to automatically write
  header if required (takes resuming into account).

_Properties_

- **total** _int, optional_: total number of lines in the file, if known through prebuffering or through the `total` kwarg.
- **headers** _casanova.Headers, optional_, optional: CSV file headers if `no_headers=False`.
- **empty** _bool_: whether the given file was empty.
- **fieldnames** _list[str], optional_: list representing the CSV file headers if `no_headers=False`.
- **row_len** _int_: expected number of items per row.
- **output_headers** _casanova.Headers, optional_, optional: output CSV headers if `no_headers=False`.
- **output_fieldnames** _list[str], optional_: list representing the output CSV headers if `no_headers=False`.
- **added_count** _int_: number of columns added to the output.

_Resuming_

A `casanova.enricher` is able to resume through a [`RowCountResumer`](#rowcountresumer) or a [`LastCellComparisonResumer`](#lastcellcomparisonresumer).

## threadsafe_enricher

Sometimes, you might want to process multiple input rows concurrently. This can mean that you will emit rows in an arbitrary order, different from the input one.

This is fine, of course, but if you still want to be able to resume an aborted process efficiently (using the [`ThreadSafeResumer](#threadsaferesumer)), your output will need specific additions for it to work, namely a column containing the index of an output row in the original input.

`casanova.threadsafe_enricher` makes it simpler by providing a tailored `writerow` method and iterators always provided the index of a row safely.

Note that such resuming is only possible if one row in the input will produce exactly one row in the output.

```python
import casanova

with open('./people.csv') as f, \
     open('./enriched-people.csv', 'w') as of:

    enricher = casanova.threadsafe_enricher(f, of, add=['age', 'hair'])

    for index, row in enricher:
        enricher.writerow(index, row, ['67', 'blond'])

    for index, value in enricher.cells('name'):
        ...

    for index, row, value in enricher.cells('name', with_rows=True):
        ...
```

_Arguments_

Everything from [`casanova.enricher`](#enricher) plus:

- **index_column** _str, optional_ [`index`]: name of the automatically added index column.

_Resuming_

A `casanova.threadsafe_enricher` is able to resume through a [`ThreadSafeResumer`](#threadsaferesumer).

## batch_enricher

Sometimes, you might want to process a CSV file and paginate API calls per row. This means that each row of your input file should produce multiple new lines, which will be written in batch each time one call from the API returns.

Sometimes, the pagination might be quite long (think collecting the Twitter followers of a very popular account), and it would not be a good idea to accumulate all the results for a single row before flushing them to file atomically because if something goes wrong, you will lose a lot of work.

But if you still want to be able to resume if process is aborted, you will need to add some things to your output. Namely, a column containing optional "cursor" data to resume your API calls and an "end" symbol indicating we finished the current input row.

```python
import casanova

with open('./twitter-users.csv') as input_file, \
     casanova.BatchResumer('./output.csv') as output_file:

    enricher = casanova.batch_resumer(input_file, output_file)

    for row in enricher:
        for results, next_cursor in paginate_api_calls(row):

            # NOTE: if we reached the end, next_cursor is None
            enricher.writebatch(row, results, next_cursor)
```

_Arguments_

Everything from [`casanova.enricher`](#enricher) plus:

- **cursor_column** _str, optional_ [`cursor`]: name of the cursor column to add.
- **end_symbol** _str, optional_ [`end`]: unambiguous (from cursor) end symbol to mark end of input row processing.

_Resuming_

A `casanova.batch_enricher` is able to resume through a [`BatchResumer`](#batchresumer).

## resumers

Through handy `Resumer` classes, `casanova` lets its enrichers and writers resume an aborted process.

Those classes must be used as a wrapper to open the output file and can assess whether resuming is actually useful or not for you.

All resumers act like file handles, can be used as a context manager using the `with` keyword and can be manually closed using the `close` method if required.

<!--
They also all accept a `listener` kwarg taking a function that will receive an event name and an event payload and that can be useful to update progress bars and such correctly when actually resuming. -->

Finally know that resumers should work perfectly well with [multiplexing](#multiplexing)

### RowCountResumer

The `RowCountResumer` works by counting the number of line of the output and skipping that many lines from the input.

It can only work in 1-to-1 scenarios where you only emit a single row per input row.

It works in `O(2n) => O(n)` time and `O(1)` memory, `n` being the number of already processed rows.

It is only supported by [`casanova.enricher`](#enricher).

```python
import casanova

with open('input.csv') as input_file, \
     casanova.RowCountResumer('output.csv') as resumer:

    # Want to know if we can resume?
    resumer.can_resume()

    # Want to know how many rows were already done?
    resumer.already_done_count()

    # Giving the resumer to an enricher as if it was the output file
    enricher = casanova.enricher(input_file, resumer)
```

### ThreadSafeResumer

`casanova` exports a threadsafe resumer that allows row to be processed concurrently and emitted in a different order.

In this precise case, couting the rows is not enough and we need to be smarter.

One way to proceed is to leverage the index column added by the threadsafe enricher to compute a set of already processed row while reading the output. Then we can just skip the input rows whose indices are in this set.

The issue here is that this consumes up to `O(n)` memory, which is prohibitive in some use cases.

To make sure this still can be done while consuming very little memory, `casanova` uses an exotic data structure we named a "contiguous range set".

This means we can resume operation in `O(n + log(h) * n)) => O(log(h) * n)` time and `O(log(h))` memory, `n` being the number of already processed rows and `h` being the size of the largest hole in the sorted indices of those same rows. Note that most of the time `h << n` since the output is mostly sorted (albeit not at a local level).

You can read more about this data structure in [this](http://yomguithereal.github.io/posts/contiguous-range-set) blog post.

Note finally this resumer can only work in 1-to-1 scenarios where you only emit a single row per input row.

It is supported by [`casanova.threadsafe_enricher`](#threadsafe_enricher) only.

```python
import casanova

with open('input.csv') as input_file, \
     casanova.ThreadSafeResumer('output.csv') as resumer:

    # Want to know if we can resume?
    resumer.can_resume()

    # Want to know how many rows were already done?
    resumer.already_done_count()

    # Giving the resumer to an enricher as if it was the output file
    enricher = casanova.threadsafe_enricher(input_file, resumer)

# If you want to use casanova ContiguousRangeSet for whatever reason
from casanova import ContiguousRangeSet
```

### BatchResumer

todo...

### LastCellResumer

Sometimes you might write an output CSV file while performing some paginated action. Said action could be aborted and you might want to resume it where you left it.

The `LastCellResumer` therefore enables you to resume writing a CSV file by reading its output's last row using a [`casanova.reverse_reader`](#reverse_reader) and extracting the value you need to resume in constant time and memory.

It is only supported by [`casanova.writer`](#writer).

```python
import casanova

with casanova.LastCellResumer('output.csv', value_column='user_id') as resumer:

    # Giving the resumer to a writer as if it was the output file
    writer = casanova.writer(resumer)

    # Extracting last relevant value if any, so we can properly resume
    last_value = resumer.get_state()
```

### LastCellComparisonResumer

In some scenarios, it is possible to resume the operation of an enricher if you can know what was the last value of some column emitted in the output.

Fortunately, using [`casanova.reverse_reader`](#reverse_reader), one can read the last line of a CSV file in constant time.

As such the `LastCellComparisonResumer` enables you to resume the work of an enricher in `O(n)` time and `O(1)` memory, with `n` being the number of already done lines that you must quickly skip when repositioning yourself in the input.

Note that it only works when the enricher emits a single line per line in the input and when the considered column value is unique across the input file.

It is only supported by [`casanova.enricher`](#enricher).

```python
import casanova

with open('input.csv') as input_file, \
     casanova.LastCellComparisonResumer('output.csv', value_colum='user_id') as resumer:

    # Giving the resumer to an enricher as if it was the output file
    enricher = casanova.enricher(input_file, resumer)
```

## count

`casanova` exposes a helper function that one can use to quickly count the number of lines in a CSV file.

```python
import casanova

count = casanova.count('./people.csv')

# You can also stop reading the file if you go beyond a number of rows
count = casanova.count('./people.csv', max_rows=100)
>>> None # if the file has more than 100 rows
>>> 34   # else the actual count

# Any additional kwarg will be passed to the underlying reader as-is
count = casanova.count('./people.csv', delimiter=';')
```

## last_cell

`casanova` exposes a helper function using a [reverse_reader](#reverse_reader) to read only the last cell value from a given column of a CSV file.

```python
import casanova

last_cell = casanova.last_cell('./people.csv', column='name')
>>> 'Quentin'

# Will return None if the file is empty
last_cell = casanova.last_cell('./empty.csv', column='name')
>>> None

# Any additional kwarg will be passed to the underlying reader as-is
last_cell = casanova.last_cell('./people.csv', column='name', delimiter=';')
```

## namedrecord

casanova's `namedrecord` is basically an enhanced & CSV-aware version of python [namedtuple](https://docs.python.org/fr/3.10/library/collections.html#collections.namedtuple).

```python
from casanova import namedrecord

Record = namedrecord(
    'Record',
    ['title', 'urls', 'is_accessible', 'data'],
    defaults=[True, None]
)

Record.fieldnames
>>> ['title', 'urls', 'is_accessible']

example = Record('Le Monde', ['https://lemonde.fr', 'https://www.lemonde.fr'])

# It works exactly like a namedtuple would, but with perks:
example
>>> Record(title='Le Monde', urls=['https://lemonde.fr', 'https://www.lemonde.fr'], is_accessible=True)

# You can read by index:
example[0]
>>> 'Le Monde'

# You can read its attributes:
example.title
>>> 'Le Monde'

# You can access it like a dict:
example['title']
>>> 'Le Monde'

# You can use #.get:
example.get('what?')
>>> None

# You can return it as a plain dict:
example.as_dict()
>>> {
    'title': 'Le Monde',
    ...
}

# You can format it as a CSV row (notice the list serialization):
example.as_csv_row():
>>> ['Le Monde', 'https://lemonde.fr|https://www.lemonde.fr', 'true']

# You can tweak formatting if needed:
example.as_csv_row(
    plural_separator='$',
    none_value='null',
    true_value='yes',
    false_value='',
)
>>> ['Le Monde', 'https://lemonde.fr$https://www.lemonde.fr', 'yes']

# You can also set default formatting options at the record level
Record = namedrecord(
    'Record',
    ['title'],
    plural_separator='$',
    ...
)

# You can format as a CSV dict row (suitable for csv.DictWriter, if required):
example.as_csv_dict_row():
>>> {
    'title': 'Le Monde',
    'urls': 'https://lemonde.fr|https://www.lemonde.fr',
    'is_accessible': 'true'
}

# You can also embed json data if you feel crazy:
Record = namedrecord(
    'Record',
    ['title', 'data'],
    json=['data']
)

example = Record('With JSON', {'hello': 'world'})

example.as_csv_row()
>>> ['With JSON', '{"hello": "world"}']
```

## set_defaults

`casanova.set_defaults` lets you edit global defaults for `casanova`:

```python
import casanova

casanova.set_defaults(strip_null_bytes_on_read=True)

# As a context manager:
with casanova.temporary_defaults(strip_null_bytes_on_read=True):
    ...
```

_Arguments_

- **strip_null_bytes_on_read** _bool, optional_ [`False`]: should readers and enrichers strip null bytes on read?
- **strip_null_bytes_on_write** _bool, optional_ [`False`]: should writers and enrichers strip null bytes on write?
- **prebuffer_bytes** _int, optional_: default prebuffer bytes for readers and enrichers.

## xsv selection mini DSL

[xsv](https://github.com/BurntSushi/xsv), a command line tool written in Rust to handle csv files, uses a clever mini DSL to let users specify column selections.

`casanova` has a working python implementation of this mini DSL that can be used by the `headers.select` method and the enrichers `select` kwargs.

Here is the gist of it (copied right from xsv documentation itself):

```
Select one column by name:
    * name

Select one column by index (1-based):
    * 2

Select the first and fourth columns:
    * 1,4

Select the first 4 columns (by index and by name):
    * 1-4
    * Header1-Header4

Ignore the first 2 columns (by range and by omission):
    * 3-
    * '!1-2'

Select the third column named 'Foo':
    * 'Foo[2]'

Re-order and duplicate columns arbitrarily:
    * 3-1,Header3-Header1,Header1,Foo[2],Header1

Quote column names that conflict with selector syntax:
    * '"Date - Opening","Date - Actual Closing"'
```
