# =============================================================================
# Casanova Enricher Unit Tests
# =============================================================================
import csv
import casanova
import pytest
from io import StringIO, BytesIO

from casanova.exceptions import (
    EmptyFileError,
    MissingColumnError
)


def collect_csv_file(path):
    with open(path) as f:
        return list(csv.reader(f))


def make_enricher_test(name, enricher_fn, binary=False):
    def get_relevant_empty_io():
        return StringIO('') if binary else BytesIO(b'')

    class AbstractTestEnricher(object):
        def test_exceptions(self):
            with pytest.raises(EmptyFileError):
                enricher_fn(get_relevant_empty_io(), get_relevant_empty_io())

        def test_basics(self, tmpdir):
            output_path = str(tmpdir.join('./enriched.csv'))

            with open('./test/resources/people.csv') as f, \
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

    return AbstractTestEnricher


TestEnricher = make_enricher_test('TestEnricher', casanova.enricher)

# if not os.environ.get('CASANOVA_TEST_SKIP_CSVMONKEY'):
#     import casanova_monkey
#     TestMonkeyEnricher = make_enricher_test('TestMonkeyEnricher', casanova_monkey.enricher, binary=True)
