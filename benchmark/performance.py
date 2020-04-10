from benchmark.utils import Timer
import click
import sys
import csv
import casanova


@click.command()
@click.argument('path')
@click.argument('column')
@click.option('--headers/--no-headers', default=True)
def bench(path, column, headers=True):
    with Timer('csv.reader'):
        with open(path) as f:
            for line in csv.reader(f):
                a = line[0]

    if headers:
        with Timer('csv.DictReader'):
            with open(path) as f:
                for line in csv.DictReader(f):
                    a = line[column]

    with Timer('casanova.reader'):
        with open(path) as f:
            for value in casanova.reader(f, column=int(column) if not headers else column, no_headers=not headers):
                a = value


if __name__ == '__main__':
    bench()
