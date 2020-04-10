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
