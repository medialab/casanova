from benchmark.utils import Timer
import click
import csv
import casanova


@click.command()
@click.argument("path")
@click.argument("column")
@click.option("--headers/--no-headers", default=True)
@click.option("--skip-std/--no-skip-std", default=True)
def bench(path, column, headers=True, skip_std=True):
    if not skip_std:
        with Timer("csv.reader"):
            with open(path) as f:
                for row in csv.reader(f):
                    a = row[0]

        if headers:
            with Timer("csv.DictReader"):
                with open(path) as f:
                    for row in csv.DictReader(f):
                        a = row[column]

    with Timer("casanova.reader: basic"):
        with open(path) as f:
            reader = casanova.reader(f, no_headers=not headers)
            for row in reader:
                a = row[reader.headers[column]]

    with Timer("casanova.reader: cached pos"):
        with open(path) as f:
            reader = casanova.reader(f, no_headers=not headers)
            pos = reader.headers[column]

            for row in reader:
                a = row[pos]

    with Timer("casanova.reader: cells"):
        with open(path) as f:
            reader = casanova.reader(f, no_headers=not headers)
            for value in reader.cells(column):
                a = value

    with Timer("casanova.reader: cells with_rows"):
        with open(path) as f:
            reader = casanova.reader(f, no_headers=not headers)
            for row, value in reader.cells(column, with_rows=True):
                a = value


if __name__ == "__main__":
    bench()
