# =============================================================================
# Casanova Resuming Strategies
# =============================================================================
#
# A collection of process resuming strategies acknowledged by casanova
# enrichers.
#
from threading import Lock
from os.path import isfile, getsize

from casanova.reader import Reader as Reader
from casanova.exceptions import ResumeError


class Resumer(object):
    def __init__(self, path, listener=None):
        self.path = path
        self.listener = listener
        self.output_file = None
        self.lock = Lock()

    def can_resume(self):
        return isfile(self.path) and getsize(self.path) > 0

    def open(self, mode='a+', encoding='utf-8', newline=''):
        return open(
            self.path,
            mode=mode,
            encoding=encoding,
            newline=newline
        )

    def open_output_file(self, **kwargs):
        if self.output_file is not None:
            raise ResumeError('output file was already opened')

        mode = 'a+' if self.can_resume() else 'w'

        self.output_file = self.open(mode=mode, **kwargs)
        return self.output_file

    def emit(self, event, payload):
        if self.listener is None:
            return

        with self.lock:
            self.listener(event, payload)

    def get_insights_from_output(self, output_reader_kwargs):
        raise NotImplementedError

    def filter_already_done_row(self, i, row):
        result = self.filter(i, row)

        if not result:
            self.emit('filter.row', (i, row))

        return result

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.output_file is None:
            raise ResumeError('resumer attempted to close unopened file')

        self.output_file.close()
        self.output_file = None

    def close(self):
        if self.output_file is not None:
            self.output_file.close()

    def __repr__(self):
        return '<{name} path={path!r} can_resume={can_resume!r}>'.format(
            name=self.__class__.__name__,
            path=self.path,
            can_resume=self.can_resume()
        )


class LineCountResumer(Resumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.line_count = 0

    def get_insights_from_output(self, output_reader_kwargs={}):
        with self.open(mode='r') as f:
            reader = Reader(f, **output_reader_kwargs)

            count = 0

            for row in reader:
                self.emit('output.row', row)
                count += 1

        self.line_count = count

    def filter(self, i, row):
        if i < self.line_count:
            return False

        return True
