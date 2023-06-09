# =============================================================================
# Casanova Resumer Unit Tests
# =============================================================================
import casanova
from casanova.resumers import Resumer, RowCountResumer


class TestResumer(object):
    def test_encoding(self):
        output_file = "./test/resources/latin_1_encoding.csv"

        resumer = RowCountResumer(output_file, encoding="latin-1")

        assert resumer.already_done_count() == 0

        with open(output_file, encoding="latin-1") as f:
            _ = casanova.enricher(f, resumer)

        assert resumer.already_done_count() == 6
