# =============================================================================
# Casanova Enricher Unit Tests
# =============================================================================
import csv
import gzip
import casanova
import pytest
import time
import sys
from io import StringIO
from collections import defaultdict
from quenouille import imap_unordered

from test.utils import collect_csv

from casanova.resuming import (
    LastCellComparisonResumer,
    RowCountResumer,
    ThreadSafeResumer
)
from casanova.exceptions import (
    EmptyFileError
)


class TestEnricher(object):
    def test_exceptions(self, tmpdir):
        with pytest.raises(EmptyFileError):
            casanova.enricher(StringIO(''), StringIO(''))

        output_path = str(tmpdir.join('./wrong_resumer.csv'))

        with pytest.raises(TypeError):
            resumer = ThreadSafeResumer(output_path)
            with open('./test/resources/people.csv') as f, resumer:
                casanova.enricher(f, resumer)

    def test_basics(self, tmpdir):
        output_path = str(tmpdir.join('./enriched.csv'))
        with open('./test/resources/people.csv') as f, \
             open(output_path, 'w', newline='') as of:
            enricher = casanova.enricher(f, of, add=('line',))

            for i, row in enumerate(enricher):
                enricher.writerow(row, [i])

        assert collect_csv(output_path) == [
            ['name', 'surname', 'line'],
            ['John', 'Matthews', '0'],
            ['Mary', 'Sue', '1'],
            ['Julia', 'Stone', '2']
        ]

    def test_dialect(self, tmpdir):
        output_path = str(tmpdir.join('./enriched.csv'))
        with open('./test/resources/semicolons.csv') as f, \
             open(output_path, 'w', newline='') as of:
            enricher = casanova.enricher(f, of, add=('line',), delimiter=';')

            for i, row in enumerate(enricher):
                enricher.writerow(row, [i])

        assert collect_csv(output_path) == [
            ['name', 'surname', 'line'],
            ['Rose', 'Philips', '0'],
            ['Luke', 'Atman', '1']
        ]

    def test_gzip(self, tmpdir):
        output_path = str(tmpdir.join('./enriched.csv'))
        with gzip.open('./test/resources/people.csv.gz', 'rt') as f, \
             open(output_path, 'w', newline='') as of:
            enricher = casanova.enricher(f, of, add=('line',))

            for i, row in enumerate(enricher):
                enricher.writerow(row, [i])

        assert collect_csv(output_path) == [
            ['name', 'surname', 'line'],
            ['John', 'Matthews', '0'],
            ['Mary', 'Sue', '1'],
            ['Julia', 'Stone', '2']
        ]

    def test_keep(self, tmpdir):
        output_path = str(tmpdir.join('./enriched_keep.csv'))
        with open('./test/resources/people.csv') as f, \
             open(output_path, 'w', newline='') as of:
            enricher = casanova.enricher(f, of, keep=('name',), add=('line',))

            for i, row in enumerate(enricher):
                enricher.writerow(row, [i])

        assert collect_csv(output_path) == [
            ['name', 'line'],
            ['John', '0'],
            ['Mary', '1'],
            ['Julia', '2']
        ]

    def test_padding(self, tmpdir):
        output_path = str(tmpdir.join('./enriched_padding.csv'))
        with open('./test/resources/people.csv') as f, \
             open(output_path, 'w', newline='') as of:
            enricher = casanova.enricher(f, of, keep=('name',), add=('line',))

            for i, row in enumerate(enricher):
                enricher.writerow(row)

        assert collect_csv(output_path) == [
            ['name', 'line'],
            ['John', ''],
            ['Mary', ''],
            ['Julia', '']
        ]

    def test_resumable(self, tmpdir):

        log = defaultdict(list)

        def listener(name, row):
            log[name].append(list(row))

        output_path = str(tmpdir.join('./enriched_resumable.csv'))

        resumer = RowCountResumer(output_path, listener=listener)

        with open('./test/resources/people.csv') as f, resumer:

            enricher = casanova.enricher(
                f, resumer,
                add=('x2',),
                keep=('name',)
            )

            row = next(iter(enricher))
            enricher.writerow(row, [2])

        assert collect_csv(output_path) == [
            ['name', 'x2'],
            ['John', '2']
        ]

        with open('./test/resources/people.csv') as f, resumer:

            enricher = casanova.enricher(
                f, resumer,
                add=('x2',),
                keep=('name',)
            )

            for i, row in enumerate(enricher):
                enricher.writerow(row, [(i + 2) * 2])

        assert collect_csv(output_path) == [
            ['name', 'x2'],
            ['John', '2'],
            ['Mary', '4'],
            ['Julia', '6']
        ]

        assert log == {
            'output.row': [['John', '2']],
            'input.row': [['John', 'Matthews']]
        }

    def test_resumable_last_cell_comparison(self, tmpdir):

        log = defaultdict(list)

        def listener(name, row):
            log[name].append(list(row))

        output_path = str(tmpdir.join('./enriched_resumable.csv'))

        resumer = LastCellComparisonResumer(output_path, value_column=0, listener=listener)

        with open('./test/resources/people.csv') as f, resumer:

            enricher = casanova.enricher(
                f, resumer,
                add=('x2',),
                keep=('name',)
            )

            row = next(iter(enricher))
            enricher.writerow(row, [2])

        assert collect_csv(output_path) == [
            ['name', 'x2'],
            ['John', '2']
        ]

        with open('./test/resources/people.csv') as f, resumer:

            enricher = casanova.enricher(
                f, resumer,
                add=('x2',),
                keep=('name',)
            )

            for i, row in enumerate(enricher):
                enricher.writerow(row, [(i + 2) * 2])

        assert collect_csv(output_path) == [
            ['name', 'x2'],
            ['John', '2'],
            ['Mary', '4'],
            ['Julia', '6']
        ]

        assert log == {'input.row': [['John', 'Matthews']]}

    def test_threadsafe(self, tmpdir):
        def job(payload):
            i, row = payload
            s = int(row[2])
            time.sleep(s * .01)

            return i, row

        output_path = str(tmpdir.join('./enriched_resumable_threadsafe.csv'))
        with open('./test/resources/people_unordered.csv') as f, \
             open(output_path, 'w', newline='') as of:

            enricher = casanova.threadsafe_enricher(
                f, of,
                add=('x2',),
                keep=('name',)
            )

            for i, row in imap_unordered(enricher, job, 3):
                enricher.writerow(i, row, [(i + 1) * 2])

        def sort_output(o):
            return sorted(tuple(i) for i in o)

        assert sort_output(collect_csv(output_path)) == sort_output([
            ['name', 'index', 'x2'],
            ['Mary', '1', '4'],
            ['Julia', '2', '6'],
            ['John', '0', '2']
        ])

    def test_threadsafe_cells(self, tmpdir):
        output_path = str(tmpdir.join('./enriched_resumable_threadsafe.csv'))
        with open('./test/resources/people_unordered.csv') as f, \
             open(output_path, 'a+') as of:

            enricher = casanova.threadsafe_enricher(
                f, of,
                add=('x2',),
                keep=('name',)
            )

            names = [t for t in enricher.cells('name')]

        assert sorted(names) == sorted([(0, 'John'), (1, 'Mary'), (2, 'Julia')])

        with open('./test/resources/people_unordered.csv') as f, \
             open(output_path, 'a+') as of:

            enricher = casanova.threadsafe_enricher(
                f, of,
                add=('x2',),
                keep=('name',)
            )

            names = [(i, v) for i, row, v in enricher.cells('name', with_rows=True)]

        assert names == [(0, 'John'), (1, 'Mary'), (2, 'Julia')]

    def test_threadsafe_resumable(self, tmpdir):
        log = defaultdict(list)

        def listener(name, row):
            log[name].append(list(row))

        def job(payload):
            i, row = payload
            s = int(row[2])
            time.sleep(s * .1)

            return i, row

        output_path = str(tmpdir.join('./enriched_resumable_threadsafe.csv'))

        resumer = ThreadSafeResumer(output_path, listener=listener)

        with open('./test/resources/people_unordered.csv') as f, resumer:

            enricher = casanova.threadsafe_enricher(
                f, resumer,
                add=('x2',),
                keep=('name',)
            )

            for j, (i, row) in enumerate(imap_unordered(enricher, job, 3)):
                enricher.writerow(i, row, [(i + 1) * 2])

                if j == 1:
                    break

        def sort_output(o):
            return sorted(tuple(i) for i in o)

        assert sort_output(collect_csv(output_path)) == sort_output([
            ['name', 'index', 'x2'],
            ['Mary', '1', '4'],
            ['Julia', '2', '6']
        ])

        with open('./test/resources/people_unordered.csv') as f, resumer:

            enricher = casanova.threadsafe_enricher(
                f, resumer,
                add=('x2',),
                keep=('name',)
            )

            for j, (i, row) in enumerate(imap_unordered(enricher, job, 3)):
                enricher.writerow(i, row, [(i + 1) * 2])

        assert sort_output(collect_csv(output_path)) == sort_output([
            ['name', 'index', 'x2'],
            ['Mary', '1', '4'],
            ['Julia', '2', '6'],
            ['John', '0', '2']
        ])

        assert sort_output(log['output.row']) == sort_output([['Mary', '1', '4'], ['Julia', '2', '6']])
        assert sort_output(log['filter.row']) == sort_output([[1, ['Mary', 'Sue', '1']], [2, ['Julia', 'Stone', '2']]])

    def test_stdout(self, capsys):
        sys.stdout.write('this,should,happen\n')
        with open('./test/resources/people.csv') as f:
            enricher = casanova.enricher(f, sys.stdout, add=('line',))

            for i, row in enumerate(enricher):
                enricher.writerow(row, [i])

        result = list(csv.reader(StringIO(capsys.readouterr().out)))

        assert result == [
            ['this', 'should', 'happen'],
            ['name', 'surname', 'line'],
            ['John', 'Matthews', '0'],
            ['Mary', 'Sue', '1'],
            ['Julia', 'Stone', '2']
        ]

    def test_combined_pos(self, tmpdir):
        output_path = str(tmpdir.join('./enriched.csv'))
        with open('./test/resources/people.csv') as f, \
             open(output_path, 'w', newline='') as of:
            enricher = casanova.enricher(f, of, add=('line',), keep=('surname',))

            assert len(enricher.output_headers) == 2
            assert enricher.output_headers.surname == 0
            assert enricher.output_headers.line == 1

    def test_batch_enricher(self, tmpdir):
        output_path = str(tmpdir.join('./enriched.csv'))
        with open('./test/resources/people.csv') as f, \
             open(output_path, 'w', newline='') as of:
            enricher = casanova.batch_enricher(f, of, add=('color',), keep=('surname',))

            for row in enricher:
                enricher.writebatch(row, [['blue'], ['red']], cursor='next')
                enricher.writebatch(row, [['purple'], ['cyan']])

        assert collect_csv(output_path) == [
            ['surname', 'cursor', 'color'],
            ['Matthews', '', 'blue'],
            ['Matthews', 'next', 'red'],
            ['Matthews', '', 'purple'],
            ['Matthews', 'end', 'cyan'],
            ['Sue', '', 'blue'],
            ['Sue', 'next', 'red'],
            ['Sue', '', 'purple'],
            ['Sue', 'end', 'cyan'],
            ['Stone', '', 'blue'],
            ['Stone', 'next', 'red'],
            ['Stone', '', 'purple'],
            ['Stone', 'end', 'cyan']
        ]
