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
  name_pos = reader.pos[0]

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

  # No headers? No problem.
  reader = casanova.reader(f, no_headers=True)
```

*casanova_monkey*

```python
import casanova_monkey

# NOTE: to rely on csvmonkey you will need to open the file in binary mode (e.g. 'rb')!
with open('./people.csv', 'rb') as f:
  reader = casanova_monkey.reader(f)

  # For the lazy, slightly faster version
  reader = casanova_monkey.reader(f, lazy=True)
```

*Arguments*

* **file** *file*: file object to read.
* **no_headers** *?bool* [`False`]: whether your CSV file is headless.
* **lazy** *?bool* [`False`]: only for `casanova_monkey`, whether to yield `csvmonkey` raw lazy-decoding items or cast them as `list` for better compatibility.

*Attributes*

* **fieldnames** *list<str>*: field names in order.
* **pos** *int|namedtuple<int>*: header positions object.

## enricher

The enricher is basically the smart combination of a `csv.reader` and a `csv.writer` and can be used to transform a given CSV file. You can then edit existing cells, add new ones and select which one from the input to keep in the output very easily, while remaining as performant as possible.

What's more, casanova's enricher are automatically resumable, meaning that if your process exits for whatever reason, it will be easy to resume where you left last time.

Also, if you need to output lines in an arbitrary order, typically when performing tasks in a multithreaded fashion (e.g. when fetching a large numbers of web pages), casanova exports a threadsafe version of its enricher. What's more, this enricher is also resumable thanks to a data structure you can read about in this blog [post](https://yomguithereal.github.io/posts/contiguous-range-set).
