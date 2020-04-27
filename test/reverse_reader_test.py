# =============================================================================
# Casanova Reverse Reader Unit Tests
# =============================================================================
import casanova
import pytest


class TestReverseReader(object):

    def test_basics(self):
        with open('./test/resources/people.csv') as f:
            reader = casanova.reverse_reader(f)

            names = list(reader.cells('name'))

        assert names == ['Julia', 'Mary', 'John']
