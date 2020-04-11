# =============================================================================
# Casanova Enricher Unit Tests
# =============================================================================
import csv
import casanova
import pytest
from io import StringIO

from casanova.exceptions import (
    EmptyFileException,
    MissingHeaderException
)


def collect_csv_file(path):
    with open(path) as f:
        return list(csv.reader(f))


class TestEnricher(object):
    def test_basics(self, tmpdir):
        with pytest.raises(EmptyFileException):
            casanova.enricher(StringIO(''), StringIO(''), column='test')

        with open('./test/resources/people.csv') as f:
            with pytest.raises(MissingHeaderException):
                casanova.enricher(f, StringIO(''), column='notfound')

        output_path = tmpdir.join('./enriched.csv')
        with open('./test/resources/people.csv') as f, \
             open(output_path, 'w') as of:
            enricher = casanova.enricher(f, of, add=('line',))

            for i, _ in enumerate(enricher):
                enricher.enrichrow([i])

        assert collect_csv_file(output_path) == [
            ['name', 'surname', 'line'],
            ['John', 'Matthews', '0'],
            ['Mary', 'Sue', '1'],
            ['Julia', 'Stone', '2']
        ]
