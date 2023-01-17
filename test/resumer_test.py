# =============================================================================
# Casanova Resumer Unit Tests
# =============================================================================
import casanova
from casanova.resuming import Resumer, RowCountResumer


class TestResumer(object):
    def test_buffer(self, tmpdir):
        output_path = str(tmpdir.join('./resumer_test.csv'))

        resumer = Resumer(output_path)

        resumer.buffer.append(list(range(2)))

        assert list(resumer) == [[0, 1]]
        assert list(resumer) == []

    def test_encoding(self):
        output_file = './test/resources/latin_1_encoding.csv'

        resumer = RowCountResumer(output_file, encoding='latin-1')

        assert resumer.already_done_count() == 0

        with open(output_file, encoding='latin-1') as f:
            enricher = casanova.enricher(f, resumer)

        assert resumer.already_done_count() == 6
