# =============================================================================
# Casanova Enricher Unit Tests
# =============================================================================
import os
import csv
import gzip
import casanova
import pytest
import time
import sys
from io import StringIO, BytesIO
from collections import defaultdict
from quenouille import imap_unordered

from casanova.resuming import (
    RowCountResumer,
    ThreadSafeResumer,
    BatchResumer
)
from casanova.exceptions import (
    EmptyFileError,
    MissingColumnError,
    NotResumableError
)


def collect_csv_file(path):
    with open(path) as f:
        return list(csv.reader(f))


def make_enricher_test(name, enricher_fn, threadsafe_enricher_fn, batch_enricher_fn,
                       binary=False):
    flag = 'r' if not binary else 'rb'
    gzip_flag = 'rt' if not binary else 'rb'

    def get_empty_io():
        return StringIO('') if not binary else BytesIO(b'')

    class AbstractTestEnricher(object):
        def test_exceptions(self, tmpdir):
            with pytest.raises(EmptyFileError):
                enricher_fn(get_empty_io(), get_empty_io())

            output_path = str(tmpdir.join('./wrong-resumer.csv'))

            with pytest.raises(TypeError):
                resumer = ThreadSafeResumer(output_path)
                with open('./test/resources/people.csv', flag) as f, resumer:
                    enricher_fn(f, resumer)

        def test_basics(self, tmpdir):
            output_path = str(tmpdir.join('./enriched.csv'))
            with open('./test/resources/people.csv', flag) as f, \
                 open(output_path, 'w') as of:
                enricher = enricher_fn(f, of, add=('line',))

                for i, row in enumerate(enricher):
                    enricher.writerow(row, [i])

            assert collect_csv_file(output_path) == [
                ['name', 'surname', 'line'],
                ['John', 'Matthews', '0'],
                ['Mary', 'Sue', '1'],
                ['Julia', 'Stone', '2']
            ]

        def test_dialect(self, tmpdir):
            if binary:
                return

            output_path = str(tmpdir.join('./enriched.csv'))
            with open('./test/resources/semicolons.csv', flag) as f, \
                 open(output_path, 'w') as of:
                enricher = enricher_fn(f, of, add=('line',), delimiter=';')

                for i, row in enumerate(enricher):
                    enricher.writerow(row, [i])

            assert collect_csv_file(output_path) == [
                ['name', 'surname', 'line'],
                ['Rose', 'Philips', '0'],
                ['Luke', 'Atman', '1']
            ]

        def test_gzip(self, tmpdir):
            output_path = str(tmpdir.join('./enriched.csv'))
            with gzip.open('./test/resources/people.csv.gz', gzip_flag) as f, \
                 open(output_path, 'w') as of:
                enricher = enricher_fn(f, of, add=('line',))

                for i, row in enumerate(enricher):
                    enricher.writerow(row, [i])

            assert collect_csv_file(output_path) == [
                ['name', 'surname', 'line'],
                ['John', 'Matthews', '0'],
                ['Mary', 'Sue', '1'],
                ['Julia', 'Stone', '2']
            ]

        def test_keep(self, tmpdir):
            output_path = str(tmpdir.join('./enriched-keep.csv'))
            with open('./test/resources/people.csv', flag) as f, \
                 open(output_path, 'w') as of:
                enricher = enricher_fn(f, of, keep=('name',), add=('line',))

                for i, row in enumerate(enricher):
                    enricher.writerow(row, [i])

            assert collect_csv_file(output_path) == [
                ['name', 'line'],
                ['John', '0'],
                ['Mary', '1'],
                ['Julia', '2']
            ]

        def test_padding(self, tmpdir):
            output_path = str(tmpdir.join('./enriched-padding.csv'))
            with open('./test/resources/people.csv', flag) as f, \
                 open(output_path, 'w') as of:
                enricher = enricher_fn(f, of, keep=('name',), add=('line',))

                for i, row in enumerate(enricher):
                    enricher.writerow(row)

            assert collect_csv_file(output_path) == [
                ['name', 'line'],
                ['John', ''],
                ['Mary', ''],
                ['Julia', '']
            ]

        def test_resumable(self, tmpdir):

            log = defaultdict(list)

            def listener(name, row):
                log[name].append(list(row))

            output_path = str(tmpdir.join('./enriched-resumable.csv'))

            resumer = RowCountResumer(output_path, listener=listener)

            with open('./test/resources/people.csv', flag) as f, resumer:

                enricher = enricher_fn(
                    f, resumer,
                    add=('x2',),
                    keep=('name',)
                )

                row = next(iter(enricher))
                enricher.writerow(row, [2])

            assert collect_csv_file(output_path) == [
                ['name', 'x2'],
                ['John', '2']
            ]

            with open('./test/resources/people.csv', flag) as f, resumer:

                enricher = enricher_fn(
                    f, resumer,
                    add=('x2',),
                    keep=('name',)
                )

                for i, row in enumerate(enricher):
                    enricher.writerow(row, [(i + 2) * 2])

            assert collect_csv_file(output_path) == [
                ['name', 'x2'],
                ['John', '2'],
                ['Mary', '4'],
                ['Julia', '6']
            ]

            assert log == {
                'output.row': [['John', '2']],
                'filter.row': [[0, ['John', 'Matthews']]]
            }

        def test_threadsafe(self, tmpdir):
            def job(payload):
                i, row = payload
                s = int(row[2])
                time.sleep(s * .01)

                return i, row

            output_path = str(tmpdir.join('./enriched-resumable-threadsafe.csv'))
            with open('./test/resources/people_unordered.csv', flag) as f, \
                 open(output_path, 'w') as of:

                enricher = threadsafe_enricher_fn(
                    f, of,
                    add=('x2',),
                    keep=('name',)
                )

                for i, row in imap_unordered(enricher, job, 3):
                    enricher.writerow(i, row, [(i + 1) * 2])

            assert collect_csv_file(output_path) == [
                ['name', 'index', 'x2'],
                ['Mary', '1', '4'],
                ['Julia', '2', '6'],
                ['John', '0', '2']
            ]

        def test_threadsafe_cells(self, tmpdir):
            def job(payload):
                i, row = payload
                s = int(row[2])
                time.sleep(s * .01)

                return i, row

            output_path = str(tmpdir.join('./enriched-resumable-threadsafe.csv'))
            with open('./test/resources/people_unordered.csv', flag) as f, \
                 open(output_path, 'a+') as of:

                enricher = threadsafe_enricher_fn(
                    f, of,
                    add=('x2',),
                    keep=('name',)
                )

                names = [t for t in enricher.cells('name')]

            assert names == [(0, 'John'), (1, 'Mary'), (2, 'Julia')]

            with open('./test/resources/people_unordered.csv', flag) as f, \
                 open(output_path, 'a+') as of:

                enricher = threadsafe_enricher_fn(
                    f, of,
                    add=('x2',),
                    keep=('name',)
                )

                names = [(i, v) for i, row, v in enricher.cells('name', with_rows=True)]

            assert names == [(0, 'John'), (1, 'Mary'), (2, 'Julia')]

        def test_threadsafe_records(self, tmpdir):
            def job(payload):
                i, row = payload
                s = int(row[2])
                time.sleep(s * .01)

                return i, row

            output_path = str(tmpdir.join('./enriched-resumable-threadsafe.csv'))
            with open('./test/resources/people_unordered.csv', flag) as f, \
                 open(output_path, 'w') as of:

                enricher = threadsafe_enricher_fn(
                    f, of,
                    add=('x2',),
                    keep=('name',)
                )

                records = [t for t in enricher.cells(['name', 'time'])]

            assert records == [(0, ['John', '3']), (1, ['Mary', '1']), (2, ['Julia', '2'])]

            with open('./test/resources/people_unordered.csv', flag) as f, \
                 open(output_path, 'w') as of:

                enricher = threadsafe_enricher_fn(
                    f, of,
                    add=('x2',),
                    keep=('name',)
                )

                records = [(i, r) for i, row, r in enricher.cells(['name', 'time'], with_rows=True)]

            assert records == [(0, ['John', '3']), (1, ['Mary', '1']), (2, ['Julia', '2'])]

        def test_threadsafe_resumable(self, tmpdir):
            log = defaultdict(list)

            def listener(name, row):
                log[name].append(list(row))

            def job(payload):
                i, row = payload
                s = int(row[2])
                time.sleep(s * .01)

                return i, row

            output_path = str(tmpdir.join('./enriched-resumable-threadsafe.csv'))

            resumer = ThreadSafeResumer(output_path, listener=listener)

            with open('./test/resources/people_unordered.csv', flag) as f, resumer:

                enricher = threadsafe_enricher_fn(
                    f, resumer,
                    add=('x2',),
                    keep=('name',)
                )

                for j, (i, row) in enumerate(imap_unordered(enricher, job, 3)):
                    enricher.writerow(i, row, [(i + 1) * 2])

                    if j == 1:
                        break

            assert collect_csv_file(output_path) == [
                ['name', 'index', 'x2'],
                ['Mary', '1', '4'],
                ['Julia', '2', '6']
            ]

            with open('./test/resources/people_unordered.csv', flag) as f, resumer:

                enricher = threadsafe_enricher_fn(
                    f, resumer,
                    add=('x2',),
                    keep=('name',)
                )

                for j, (i, row) in enumerate(imap_unordered(enricher, job, 3)):
                    enricher.writerow(i, row, [(i + 1) * 2])

            assert collect_csv_file(output_path) == [
                ['name', 'index', 'x2'],
                ['Mary', '1', '4'],
                ['Julia', '2', '6'],
                ['John', '0', '2']
            ]

            assert log == {
                'output.row': [['Mary', '1', '4'], ['Julia', '2', '6']],
                'filter.row': [[1, ['Mary', 'Sue', '1']], [2, ['Julia', 'Stone', '2']]]
            }

        def test_stdout(self, capsys):
            sys.stdout.write('this,should,happen\n')
            with open('./test/resources/people.csv', flag) as f:
                enricher = enricher_fn(f, sys.stdout, add=('line',))

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
            with open('./test/resources/people.csv', flag) as f, \
                 open(output_path, 'w') as of:
                enricher = enricher_fn(f, of, add=('line',), keep=('surname',))

                assert len(enricher.output_pos) == 2
                assert enricher.output_pos.surname == 0
                assert enricher.output_pos.line == 1

        def test_batch_enricher(self, tmpdir):
            output_path = str(tmpdir.join('./enriched.csv'))
            with open('./test/resources/people.csv', flag) as f, \
                 open(output_path, 'w') as of:
                enricher = batch_enricher_fn(f, of, add=('color',), keep=('surname',))

                for row in enricher:
                    enricher.writebatch(row, [['blue'], ['red']], cursor='next')
                    enricher.writebatch(row, [['purple'], ['cyan']])

            assert collect_csv_file(output_path) == [
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

    return AbstractTestEnricher


TestEnricher = make_enricher_test(
    'TestEnricher',
    casanova.enricher,
    casanova.threadsafe_enricher,
    casanova.batch_enricher
)

if not os.environ.get('CASANOVA_TEST_SKIP_CSVMONKEY'):
    import casanova_monkey
    TestMonkeyEnricher = make_enricher_test(
        'TestMonkeyEnricher',
        casanova_monkey.enricher,
        casanova_monkey.threadsafe_enricher,
        casanova_monkey.batch_enricher,
        binary=True
    )
