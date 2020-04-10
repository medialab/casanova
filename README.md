[![Build Status](https://travis-ci.org/medialab/casanova.svg)](https://travis-ci.org/medialab/casanova)

# Casanova

If you often find yourself reading CSV files using python, you will quickly notice that, while being more comfortable, `csv.DictReader` remains way slower than `csv.reader`:

```
# To read a 1.5G CSV file:
csv.reader: 24s
csv.DictReader: 84s
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

## Usage

* [reader](#reader)

## reader

```python
import casanova

with open('./file.csv') as f:

  # Interested in a single column?
  for url in casanova.reader(f, column='url'):
    print(url)

  # No headers?
  for url in casanova.reader(f, column=0, no_headers=True):
    print(url)

  # Interested in several columns
  for title, url in casanova.reader(f, columns=('title', 'url')):
    print(title, url)

  # Working on records
  for record in casanova.reader(f, columns=('title', 'url')):
    # record is a namedtuple based on your columns
    print(record[0], record.url)

  # Records slow you down? Need to go faster?
  # You can iterate directly on rows and use the reader's recorded positions
  reader = casanova.reader(f, columns=('title', 'url'))
  url_pos = reader.pos.url

  for row in reader.rows():
    print(row[url_pos])
```

### Arguments

* **file** *file*: file object to read.
* **column** *?str|int*: name or index of target column.
* **colums** *?iterable<str|int>*: iterable of name or index of target columns.

### Attributes

* *pos* *int|namedtuple<int>*: index of target column or named tuple of target columns.

### Methods

#### __iter__

Lets you iterate on a single value or on a namedtuple record.

#### rows

Lets you iterate over the original `csv.reader`.
