# =============================================================================
# Casanova Writer Unit Tests
# =============================================================================
from io import StringIO

from test.utils import collect_csv

from casanova.writer import Writer
from casanova.resuming import LastCellResumer


class TestWriter(object):
    def test_basics(self):
        output = StringIO()
        writer = Writer(output, ['name', 'surname'])
        writer.writerow(['John', 'Cage'])
        writer.writerow(['Julia', 'Andrews'])

        assert collect_csv(output) == [
            ['name', 'surname'],
            ['John', 'Cage'],
            ['Julia', 'Andrews']
        ]

    def test_resumable(self, tmpdir):
        output_path = str(tmpdir.join('./written_resumable.csv'))

        def stream(offset=0):
            return range(offset, 6)

        with LastCellResumer(output_path, 'index') as resumer:
            writer = Writer(resumer, ['index'])

            for i in stream(resumer.get_state() or 0):
                if i == 3:
                    break
                writer.writerow([i])

        assert collect_csv(output_path) == [['index'], ['0'], ['1'], ['2']]

        with LastCellResumer(output_path, 'index') as resumer:
            writer = Writer(resumer, ['index'])

            assert resumer.get_state() == '2'
            n = resumer.pop_state()

            assert n == '2'
            assert resumer.pop_state() is None

            for i in stream(int(n) + 1):
                writer.writerow([i])

        assert collect_csv(output_path) == [['index'], ['0'], ['1'], ['2'], ['3'], ['4'], ['5']]
