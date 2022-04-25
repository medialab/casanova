# =============================================================================
# Casanova Resumer Unit Tests
# =============================================================================
from casanova.resuming import Resumer


class TestResumer(object):
    def test_buffer(self, tmpdir):
        output_path = str(tmpdir.join('./resumer_test.csv'))

        resumer = Resumer(output_path)

        resumer.buffer.append(list(range(2)))

        assert list(resumer) == [[0, 1]]
        assert list(resumer) == []
