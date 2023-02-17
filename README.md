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

`casanova` also packs exotic utilities able to read csv files in reverse (in constant time), so you can fetch useful information to restart some aborted process.

## Installation

You can install `casanova` with pip with the following command:

```
pip install casanova
```

## Usage

- [reader](#reader)
- [headers](#headers)
- [count](#count)
- [enricher](#enricher)
- [threadsafe_enricher](#threadsafe_enricher)
- [reverse_reader](#reverse_reader)
- [namedrecord](#namedrecord)
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

    # No headers? No problem.
    reader = casanova.reader(f, no_headers=True)

# Note that you can also create a reader from a path
with casanova.reader('./people.csv') as reader:
  ...

# And if you need exotic encodings
with casanova.reader('./people.csv', encoding='latin1') as reader:
  ...

# The reader will also handle gzipped files out of the box
with casanove.reader('./people.csv.gz') as reader:
  ...

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
- **multiplex** _casanova.Multiplexer, optional_: multiplexer to use. See relevant documentation hereafter for more information.
- **strip_null_bytes_on_read** _bool, optional_ [`False`]: before python 3.11, the `csv` module will raise when attempting to read a CSV file containing null bytes. If set to `True`, the reader will strip null bytes on the fly while parsing rows.
- **reverse** _bool, optional_ [`False`]: whether to read the file in reverse (except for the header of course).

_Properties_

- **total** _int, optional_: total number of lines in the file, if known through prebuffering or through the `total` kwarg.
- **headers** _casanova.Headers_, optional: CSV file headers if `no_headers=False`.
- **empty** _bool_: whether the given file was empty.
- **fieldnames** _list[str], optional_: list representing the CSV file headers if `no_headers=False`.
- **row_len** _int_: expected number of items per row.

_Methods_

- **rows**: returns an iterator over the reader rows. Same as iterating over the reader directly.
- **cells**: take the name of a column or its position and returns an iterator over values of the given column. Can be given `with_rows=True` if you want to iterate over a `value, row` tuple instead if required.
- **enumerate**: resuming-safe enumeration over the rows yielding `index, row` tuples.
- **wrap**: method taking a list row and returning a `DictLikeRow` object to wrap it.
- **close**: cleans up the reader resources manually when not using the dedicated context manager. It is usually only useful when the reader was given a path and not an already opened file handle.

_Multiplexing_

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

# Renaming a column:
headers.rename('name', 'first_name')

# Couting columns:
len(headers)
>>> 3

# Retrieving the nth header:
headers.nth(1)
>>> 'surname'

# Wraping a row
headers.wrap(['John', 'Matthews', '45'])
>>> DictLikeRow(name='John', surname='Matthews', age='45')

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

## count

`casanova` exposes a helper function that one can use to quickly count the number of lines in a CSV file.

```python
import casanova

count = casanova.count('./people.csv')

# You can also stop reading the file if you go beyond a number of rows
count = casanova.count('./people.csv', max_rows=100)
>>> None # if the file has more than 100 rows
>>> 34   # else the actual count
```

## enricher

The enricher is basically a smart combination of a `csv.reader` and a `csv.writer`. It can be used to transform a given CSV file. You can then edit existing cells, add new ones and select which one from the input to keep in the output very easily, while remaining as performant as possible.

What's more, casanova's enrichers are automatically resumable, meaning that if your process exits for whatever reason, it will be easy to restart where you left last time.

Also, if you need to output lines in an arbitrary order, typically when performing tasks in a multithreaded fashion (e.g. when fetching a large numbers of web pages), casanova exports a threadsafe version of its enricher. This enricher is also resumable thanks to a data structure you can read about in this blog [post](https://yomguithereal.github.io/posts/contiguous-range-set).

Resuming typically requires `O(n)` time (sometime constant time when able to use a reverse reader), `n` being the number of lines already done but only consumes amortized `O(1)` memory.

```python
import casanova

with open('./people.csv') as f, \
     open('./enriched-people.csv', 'w') as of:

    enricher = casanova.enricher(f, of)

    # The enricher inherits from casanova.reader
    enricher.headers
    >>> Headers(name=0, surname=1)

    # You can iterate over its rows
    name_pos = enricher.headers.name
    for row in enricher:

        # Editing a cell, so that everyone is called John
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

    # You can of course still use #.cells
    for row, name in enricher.cells('name', with_rows=True):
        print(row, name)
```

_Arguments_

- **input_file** _file|str_: file object to read or path to open.
- **output_file** _file|Resumer_: file object to write.
- **no_headers** _?bool_ [`False`]: whether your CSV file is headless.
- **add** _?iterable<str|int>_: names of columns to add to output.
- **select** _?iterable<str|int>|str_: names of colums to keep from input.

_Resuming an enricher_

```python
import casanova
from casanova import RowCountResumer

with open('./people.csv') as f, \
     RowCountResumer('./enriched-people.csv') as resumer:

    # This will automatically start where it stopped last time
    enricher = casanova.enricher(f, resumer)

    for row in enricher:
        row[1] = 'John'
        enricher.writerow(row)

# You can also listen to events if you need to advance loading bars etc.
def listener(event, row):
    print(event, row)

resumer = RowCountResumer('./enriched-people.csv', listener=listener)

# You can check if the process was already started and can resume:
resumer.can_resume()

# You can check how many lines were already processed:
resumer.already_done_count()
```

## threadsafe_enricher

To safely resume, the threadsafe version needs you to add an index column to the output so we can make sense of what was already done. Therefore, its `writerow` method is a bit different because it takes an additional argument being the original index of the row you need to enrich.

To help you doing so, all the enricher's iteration methods therefore yield the index alongside the row.

Note finally that resuming is only possible if one line in the input is meant to produce exactly one line in the output.

```python
import casanova

with open('./people.csv') as f, \
     open('./enriched-people.csv', 'w') as of:

    enricher = casanova.threadsafe_enricher(f, of, add=['age', 'hair'])

    for index, row in enricher:
        enricher.writerow(index, row, ['67', 'blond'])

# With resuming:
from casanova import ThreadSafeResumer

with open('./people.csv') as f, \
     ThreadSafeResumer('./enriched-people.csv') as resumer:

    enricher = casanova.threadsafe_enricher(f, resumer, add=['age', 'hair'])
```

_Threadsafe arguments_

- **index_column** _str, optional_ [`index`]: name of the index column.

## reverse_reader

casanova's reverse reader lets you read a CSV file backwards while still parsing its headers first. It looks silly but it is very useful if you need to read the last lines of a CSV file in constant time & memory when resuming some process.

It is basically identical to `casanova.reader` except lines will be yielded in reverse.

```python
import casanova

with open('./people.csv', 'rb') as f:
    reader = casanova.reverse_reader(f)

    next(reader)
    >>> ['Mr. Last', 'Line']

# It also comes with a static helper if you only need to read last cell
last_surname = casanova.reverse_reader.last_cell('./people.csv', 'surname')
>>> 'Mr. Last'
```

## namedrecord

casanova's `namedrecord` is basically an enhanced & CSV-aware version of python [namedtuple](https://docs.python.org/fr/3.10/library/collections.html#collections.namedtuple).

```python
from casanova import namedrecord

Record = namedrecord(
    'Record',
    ['title', 'urls', 'is_accessible', 'data'],
    defaults=[True, None],
    boolean=['is_accessible'],
    plural=['urls']
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

# You can format it as a CSV row:
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
