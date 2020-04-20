from benchmark.utils import Timer
import click
import sys
import csv
import casanova
import csvmonkey


@click.command()
@click.argument('path')
@click.argument('column')
@click.option('--headers/--no-headers', default=True)
def bench(path, column, headers=True):
    with Timer('csvmonkey'):
        with open(path, 'rb') as f:
            for line in csvmonkey.from_file(f, header=headers):
                a = line[column]

    with Timer('csv.reader'):
        with open(path) as f:
            for line in csv.reader(f):
                a = line[0]

    if headers:
        with Timer('csv.DictReader'):
            with open(path) as f:
                for line in csv.DictReader(f):
                    a = line[column]

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

if __name__ == '__main__':
    bench()
