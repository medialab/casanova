[![Build Status](https://travis-ci.org/medialab/casanova.svg)](https://travis-ci.org/medialab/casanova)

# Casanova

If you often find yourself reading CSV files using python, you will quickly notice that, while being more comfortable, `csv.DictReader` way slower than `csv.reader`:

```
# In seconds, to read a 1.5G CSV file:
csv.reader: 24.7295156020009
csv.DictReader: 84.00519230200007
```

Casanova is therefore an attempt to stick to `csv.reader` performance while still keeping a comfortable interface, able to mind headers etc.

Thus, Casanova is a good fit for you if you need to:

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
