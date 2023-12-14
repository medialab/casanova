# =============================================================================
# Casanova Inferring Enricher Unit Tests
# =============================================================================
from io import StringIO

from test.utils import collect_csv

from casanova.enricher import InferringEnricher


class TestInferringEnricher(object):
    def test_basics(self):
        with open("./test/resources/people.csv") as f:
            output = StringIO()
            enricher = InferringEnricher(f, output)

            for i, row in enricher.enumerate():
                enricher.writerow(row, {"index": i + 10})

        assert collect_csv(output) == [
            ["name", "surname", "index"],
            ["John", "Matthews", "10"],
            ["Mary", "Sue", "11"],
            ["Julia", "Stone", "12"],
        ]

    def test_select(self):
        with open("./test/resources/people.csv") as f:
            output = StringIO()
            enricher = InferringEnricher(f, output, select="surname")

            for i, row in enricher.enumerate():
                enricher.writerow(row, {"index": i + 10})

        assert collect_csv(output) == [
            ["surname", "index"],
            ["Matthews", "10"],
            ["Sue", "11"],
            ["Stone", "12"],
        ]
