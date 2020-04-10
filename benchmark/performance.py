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

    with Timer('casanova.reader: value'):
        with open(path) as f:
            for value in casanova.reader(f, column=int(column) if not headers else column, no_headers=not headers):
                a = value

    with Timer('casanova.reader: record destructuring'):
        with open(path) as f:
            for record, in casanova.reader(f, columns=[int(column) if not headers else column], no_headers=not headers):
                a = record

    with Timer('casanova.reader: record pos'):
        with open(path) as f:
            reader = casanova.reader(f, columns=[int(column) if not headers else column], no_headers=not headers)
            pos = reader.pos[0]

            for record in reader:
                a = record[pos]

    with Timer('casanova.reader: record attr'):
        with open(path) as f:
            reader = casanova.reader(f, columns=[int(column) if not headers else column], no_headers=not headers)

            for record in reader:
                a = record.url

    with Timer('casanova.reader: raw pos'):
        with open(path) as f:
            reader = casanova.reader(f, column=int(column) if not headers else column, no_headers=not headers)

            for row in reader.rows():
                a = row[reader.pos]

if __name__ == '__main__':
    bench()
