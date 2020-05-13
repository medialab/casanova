[![Build Status](https://travis-ci.org/medialab/casanova.svg)](https://travis-ci.org/medialab/casanova)

# Casanova

If you often find yourself reading CSV files using python, you will quickly notice that, while being more comfortable, `csv.DictReader` remains way slower than `csv.reader`:

```
# To read a 1.5G CSV file:
csv.reader: 24s
csv.DictReader: 84s
casanova.reader: 25s
csvmonkey: 3s
casanova_monkey.reader: 4s
```

Casanova is therefore an attempt to stick to `csv.reader` performance while still keeping a comfortable interface, still able to consider headers etc.

Casanova is thus a good fit for you if you need to:

* Stream large CSV files without running out of memory
* Enrich the same CSV files by outputing a similar file, all while adding, filtering and editing cells.
* Have the possibility to resume said enrichment if your process exited
* Do so in a threadsafe fashion, and be able to resume even if your output does not have the same order as the input

## Installation

You can install `casanova` with pip with the following command:

```
pip install casanova
```

If you want to be able to use the faster `casanova_monkey` namespace relying on the fantastic [csvmonkey](https://github.com/dw/csvmonkey) library, you will also need to install it alongside:

```
pip install csvmonkey
# If this fails, typically on ubuntu, run the following:
sudo apt-get install clang
CC=clang pip install csvmonkey
```

or you can also install `casanova` likewise:

```
pip install casanova[monkey]
```

## Usage

* [reader](#reader)
* [enricher](#enricher)
* [reverse_reader](#reverse_reader)

## reader

Straightforward CSV reader exposing some information and indices about the given file's headers.

```python
import casanova

with open('./people.csv') as f:

  # Creating a reader
  reader = casanova.reader(f)

  # Getting header information
  reader.fieldnames
  >>> ['name', 'surname']

  reader.pos
  >>> HeadersPositions(name=0, surname=1)

  name_pos = reader.pos.name
  name_pos = reader.pos['name']

  'name' in reader.pos
  >>> True

  # Iterating over the rows
  for row in reader:
    name = row[name_pos] # it's better to cache your pos outside the loop
    name = row[reader.pos.name] # this works, but is slower

  # Intersted in a single column?
  for name in reader.cells('name'):
    print(name)

  # Interested in several columns (handy but has a slight perf cost!)
  for name, surname in reader.cells(['name', 'surname']):
    print(name, surname)

  # Need also the current row when iterating on cells?
  for row, (name, surname) in reader.cells(['name', 'surname']):
    print(row, name, surname)

  # No headers? No problem.
  reader = casanova.reader(f, no_headers=True)

# Note that you can also create a reader from a path
with casanova.reader('./people.csv') as reader:
  pass

# And if you need exotic encodings
with casanova.reader('./people.csv', encoding='latin1') as reader:
  pass

# Readers can also be closed if you want to avoid context managers
reader.close()
```

*Counting number of rows in a CSV file*

To do so quickly you can use `casanova.reader` static `count` method.

```python
import casanova

count = casanova.reader.count('./people.csv')

# You can also stop reading the file if you go beyond a number of rows
count = casanova.reader.count('./people.csv', max_rows=100)
>>> None # if the file has more than 100 rows
>>> 34   # else the actual count
```

*casanova_monkey*

```python
import casanova_monkey

# NOTE: to rely on csvmonkey you will need to open the file in binary mode (e.g. "rb")!
with open('./people.csv', 'rb') as f:
  reader = casanova_monkey.reader(f)

  # For the lazy, slightly faster version
  reader = casanova_monkey.reader(f, lazy=True)
```

*Arguments*

* **file** *file|path*: file object to read or path to open.
* **no_headers** *?bool* [`False`]: whether your CSV file is headless.
* **lazy** *?bool* [`False`]: only for `casanova_monkey`, whether to yield `csvmonkey` raw lazy-decoding items or cast them as `list` for better compatibility.

*Attributes*

* **fieldnames** *list<str>*: field names in order.
* **pos** *int|namedtuple<int>*: header positions object.

## enricher

The enricher is basically a smart combination of a `csv.reader` and a `csv.writer`. It can be used to transform a given CSV file. You can then edit existing cells, add new ones and select which one from the input to keep in the output very easily, while remaining as performant as possible.

What's more, casanova's enrichers are automatically resumable, meaning that if your process exits for whatever reason, it will be easy to restart where you left last time.

Also, if you need to output lines in an arbitrary order, typically when performing tasks in a multithreaded fashion (e.g. when fetching a large numbers of web pages), casanova exports a threadsafe version of its enricher. This enricher is also resumable thanks to a data structure you can read about in this blog [post](https://yomguithereal.github.io/posts/contiguous-range-set).

Resuming typically requires `O(n)` time, `n` being the number of lines already done but only consumes amortized `O(1)` memory.

```python
import casanova

with open('./people.csv') as f, \
     open('./enriched-people.csv', 'w') as of:
  enricher = casanova.enricher(f, of)

  # The enricher inherits from casanova.reader
  enricher.pos
  >>> HeadersPositions(name=0, surname=1)

  # You can iterate over its rows
  name_pos = enricher.pos.name
  for row in enricher:

    # Editing a cell, so that everyone is called John
    row[name_pos] = 'John'
    enricher.writerow(row)

  # Want to add columns?
  enricher = casanova.enricher(f, of, add=['age', 'hair'])

  for row in enricher:
    enricher.writerow(row, ['34', 'blond'])

  # Want to keep only some columns from input?
  enricher = casanova.enricher(f, of, add=['age'], keep=['surname'])

  for row in enricher:
    enricher.writerow(row, ['45'])

  # You can of course still use #.cells
  for row, name in enricher.cells('name', with_rows=True):
    print(row, name)
```

*Arguments*

* **input_file** *file|str*: file object to read or path to open.
* **output_file** *file*: file object to write.
* **no_headers** *?bool* [`False`]: whether your CSV file is headless.
* **add** *?iterable<str|int>*: names of columns to add to output.
* **keep** *?iterable<str|int>*: names of colums to keep from input.
* **resumable** *?bool* [`False`]: whether the enricher should be able to resume.
* **listener** *?callable*: a function listening to the enricher's events.

*Resuming an enricher*

```python
import casanova

# NOTE: to be able to resume you will need to open the output file with "a+"
with open('./people.csv') as f, \
     open('./enriched-people.csv', 'a+') as of:

  # This will automatically start where it stopped last time
  enricher = casanova.enricher(f, of, resumable=True)

  for row in enricher:
    row[1] = 'John'
    enricher.writerow(row)

  # You can also listen to events if you need to advance loading bars etc.
  def listener(event, row):
    print(event, row)

  enricher = casanova.enricher(f, of, resumable=True, listener=listener)

  # Want more control over resuming?
  enricher = casanova.enricher(f, of, resumable=True, auto_resume=False)

  # You will then need to call #.resume yourself
  enricher.should_resume
  >>> True

  enricher.resume()

  # Knowing how many lines were already processed
  enricher.already_done_count
  >>> 45
```

*Threadsafe version*

To be safely resumable, the threadsafe version needs you to add an index column to the output so we can make sense of what was already done. Therefore, its `writerow` method is a bit different because it takes an additional argument being the original index of the row you need to enrich.

To help you doing so, all the enricher's iteration methods therefore yield the index alongside the row.

Note finally that resuming is only possible if one line in the input is meant to produce exactly one line in the output.

```python
import casanova

with open('./people.csv') as f, \
     open('./enriched-people.csv', 'w') as of:

  enricher = casanova.threadsafe_enricher(f, of, add=['age', 'hair'])

  for index, row in enricher:
    enricher.writerow(index, row, ['67', 'blond'])
```

*Threadsafe arguments*

* **index_column** *?str* [`index`]: name of the index column.

*casanova_monkey*

```python
import casanova_monkey

with open('./people.csv') as f, \
     open('./enriched-people.csv', 'w') as of:

  enricher = casanova_monkey.enricher(f, of)
  enricher = casanova_monkey.threadsafe_enricher(f, of)
```

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
