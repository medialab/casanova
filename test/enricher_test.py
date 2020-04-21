# =============================================================================
# Casanova Enricher Unit Tests
# =============================================================================
import os
import csv
import casanova
import pytest
import time
from io import StringIO, BytesIO
from collections import defaultdict
from quenouille import imap_unordered

from casanova.exceptions import (
    EmptyFileError,
    MissingColumnError,
    NotResumableError
)


def collect_csv_file(path):
    with open(path) as f:
        return list(csv.reader(f))


def make_enricher_test(name, enricher_fn, threadsafe_enricher_fn, binary=False):
    flag = 'r' if not binary else 'rb'

    def get_empty_io():
        return StringIO('') if not binary else BytesIO(b'')

    class AbstractTestEnricher(object):
        def test_exceptions(self, tmpdir):
            with pytest.raises(EmptyFileError):
                enricher_fn(get_empty_io(), get_empty_io())

            with pytest.raises(NotResumableError):
                output_path = str(tmpdir.join('./enriched-not-resumable.csv'))
                with open('./test/resources/people.csv', flag) as f, \
                     open(output_path, 'w') as of:
                    enricher = enricher_fn(f, of, resumable=True)

        def test_basics(self, tmpdir):
            output_path = str(tmpdir.join('./enriched.csv'))
            with open('./test/resources/people.csv', flag) as f, \
                 open(output_path, 'w') as of:
                enricher = enricher_fn(f, of, add=('line',))

                for i, row in enumerate(enricher):
                    enricher.enrichrow(row, [i])

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
                    enricher.enrichrow(row, [i])

            assert collect_csv_file(output_path) == [
                ['name', 'line'],
                ['John', '0'],
                ['Mary', '1'],
                ['Julia', '2']
            ]

        def test_resumable(self, tmpdir):

            log = defaultdict(list)

            def listener(name, row):
                log[name].append(list(row))

            output_path = str(tmpdir.join('./enriched-resumable.csv'))
            with open('./test/resources/people.csv', flag) as f, \
                 open(output_path, 'a+') as of:

                enricher = enricher_fn(
                    f, of,
                    add=('x2',),
                    keep=('name',),
                    resumable=True,
                    listener=listener
                )

                row = next(iter(enricher))
                enricher.enrichrow(row, [2])

            assert collect_csv_file(output_path) == [
                ['name', 'x2'],
                ['John', '2']
            ]

            with open('./test/resources/people.csv', flag) as f, \
                 open(output_path, 'a+') as of:

                enricher = enricher_fn(
                    f, of,
                    add=('x2',),
                    keep=('name',),
                    resumable=True,
                    listener=listener
                )

                for i, row in enumerate(enricher):
                    enricher.enrichrow(row, [(i + 2) * 2])

            assert collect_csv_file(output_path) == [
                ['name', 'x2'],
                ['John', '2'],
                ['Mary', '4'],
                ['Julia', '6']
            ]

            assert log == {
                'resume.output': [['John', '2']],
                'resume.input': [['John', 'Matthews']]
            }

        def test_threadsafe(self, tmpdir):
            def job(payload):
                i, row = payload
                s = int(row[2])
                time.sleep(s * .1)

                return i, row

            output_path = str(tmpdir.join('./enriched-resumable-threadsafe.csv'))
            with open('./test/resources/people-unordered.csv', flag) as f, \
                 open(output_path, 'a+') as of:

                enricher = threadsafe_enricher_fn(
                    f, of,
                    add=('x2',),
                    keep=('name',)
                )

                for i, row in imap_unordered(enricher, job, 3):
                    enricher.enrichrow(i, row, [(i + 1) * 2])

            assert collect_csv_file(output_path) == [
                ['index', 'name', 'x2'],
                ['1', 'Mary', '4'],
                ['2', 'Julia', '6'],
                ['0', 'John', '2']
            ]

        def test_threadsafe_cells(self, tmpdir):
            def job(payload):
                i, row = payload
                s = int(row[2])
                time.sleep(s * .1)

                return i, row

            output_path = str(tmpdir.join('./enriched-resumable-threadsafe.csv'))
            with open('./test/resources/people-unordered.csv', flag) as f, \
                 open(output_path, 'a+') as of:

                enricher = threadsafe_enricher_fn(
                    f, of,
                    add=('x2',),
                    keep=('name',)
                )

                names = [t for t in enricher.cells('name')]

            assert names == [(0, 'John'), (1, 'Mary'), (2, 'Julia')]

        def test_threadsafe_records(self, tmpdir):
            def job(payload):
                i, row = payload
                s = int(row[2])
                time.sleep(s * .1)

                return i, row

            output_path = str(tmpdir.join('./enriched-resumable-threadsafe.csv'))
            with open('./test/resources/people-unordered.csv', flag) as f, \
                 open(output_path, 'a+') as of:

                enricher = threadsafe_enricher_fn(
                    f, of,
                    add=('x2',),
                    keep=('name',)
                )

                records = [t for t in enricher.cells(['name', 'time'])]

            assert records == [(0, (0, ['John', '3'])), (1, (1, ['Mary', '1'])), (2, (2, ['Julia', '2']))]

    return AbstractTestEnricher


TestEnricher = make_enricher_test(
    'TestEnricher',
    casanova.enricher,
    casanova.threadsafe_enricher
)

if not os.environ.get('CASANOVA_TEST_SKIP_CSVMONKEY'):
    import casanova_monkey
    TestMonkeyEnricher = make_enricher_test(
        'TestMonkeyEnricher',
        casanova_monkey.enricher,
        casanova_monkey.threadsafe_enricher,
        binary=True
    )
