from benchmark.utils import Timer
import click
import sys
import csv
import casanova
import casanova_monkey
import csvmonkey


@click.command()
@click.argument('path')
@click.argument('column')
@click.option('--headers/--no-headers', default=True)
@click.option('--skip-std/--no-skip-std', default=True)
def bench(path, column, headers=True, skip_std=True):
    if not skip_std:
        with Timer('csvmonkey'):
            with open(path, 'rb') as f:
                for row in csvmonkey.from_file(f, header=headers):
                    a = row[column]

    with Timer('casanova_monkey: basic'):
        with open(path, 'rb') as f:
            reader = casanova_monkey.reader(f, no_headers=not headers)
            pos = reader.pos[column]

            for row in reader:
                a = row[pos]

    with Timer('casanova_monkey: lazy'):
        with open(path, 'rb') as f:
            reader = casanova_monkey.reader(f, no_headers=not headers, lazy=True)
            pos = reader.pos[column]

            for row in reader:
                a = row[pos]

    if not skip_std:
        with Timer('csv.reader'):
            with open(path) as f:
                for row in csv.reader(f):
                    a = row[0]

        if headers:
            with Timer('csv.DictReader'):
                with open(path) as f:
                    for row in csv.DictReader(f):
                        a = row[column]

    with Timer('casanova.reader: basic'):
        with open(path) as f:
            reader = casanova.reader(f, no_headers=not headers)
            for row in reader:
                a = row[reader.pos[column]]

    with Timer('casanova.reader: cached pos'):
        with open(path) as f:
            reader = casanova.reader(f, no_headers=not headers)
            pos = reader.pos[column]

            for row in reader:
                a = row[pos]

    with Timer('casanova.reader: cells'):
        with open(path) as f:
            reader = casanova.reader(f, no_headers=not headers)
            for value in reader.cells(column):
                a = value

    with Timer('casanova.reader: cells with_rows'):
        with open(path) as f:
            reader = casanova.reader(f, no_headers=not headers)
            for row, value in reader.cells(column, with_rows=True):
                a = value

    with Timer('casanova.reader: records'):
        with open(path) as f:
            reader = casanova.reader(f, no_headers=not headers)
            for value, in reader.cells([column]):
                a = value

if __name__ == '__main__':
    bench()
