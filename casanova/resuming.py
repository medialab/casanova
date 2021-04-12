# =============================================================================
# Casanova Resuming Strategies
# =============================================================================
#
# A collection of process resuming strategies acknowledged by casanova
# enrichers.
#
from os.path import isfile, getsize

from casanova.reader import CasanovaReader as Reader


class Resumer(object):
    def __init__(self, path):
        self.path = path
        self.can_resume = isfile(path) and getsize(path) > 0

    def open(self, mode='a+', encoding='utf-8', newline='', binary=False):
        if binary:
            mode += 'b'

        return open(
            self.path,
            mode=mode,
            encoding=encoding,
            newline=newline
        )

    def get_insights_from_output(self, reader_kwargs):
        raise NotImplementedError

    def filter_already_done_row(self, i, row):
        raise NotImplementedError


class LineCountResumer(Resumer):
    def __init__(self, path, listener=None):
        super().__init__(path)
        self.line_count = 0
        self.listener = listener

    def get_insights_from_output(self, reader_kwargs):
        with self.open(mode='r') as f:
            reader = Reader(f, **reader_kwargs)

            count = 0

            for row in reader:

                if callable(self.listener):
                    self.listener(row)

                count += 1

        self.line_count = count

    def filter_already_done_row(self, i, row):
        if i < self.line_count:
            return False

        return True
