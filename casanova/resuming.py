# =============================================================================
# Casanova Resuming Strategies
# =============================================================================
#
# A collection of process resuming strategies acknowledged by casanova
# enrichers.
#
from threading import Lock
from os.path import isfile, getsize

from casanova.reader import Reader
from casanova.reverse_reader import ReverseReader
from casanova.exceptions import (
    ResumeError,
    MissingColumnError,
    CorruptedIndexColumn
)
from casanova.contiguous_range_set import ContiguousRangeSet


class Resumer(object):
    def __init__(self, path, listener=None):
        self.path = path
        self.listener = listener
        self.output_file = None
        self.lock = Lock()

    def can_resume(self):
        return isfile(self.path) and getsize(self.path) > 0

    def open(self, mode='a', encoding='utf-8', newline=''):
        return open(
            self.path,
            mode=mode,
            encoding=encoding,
            newline=newline
        )

    def open_output_file(self, **kwargs):
        if self.output_file is not None:
            raise ResumeError('output file is already opened')

        mode = 'a+' if self.can_resume() else 'w'

        self.output_file = self.open(mode=mode, **kwargs)
        return self.output_file

    def emit(self, event, payload):
        if self.listener is None:
            return

        with self.lock:
            self.listener(event, payload)

    def get_insights_from_output(self, enricher):
        raise NotImplementedError

    def filter_already_done_row(self, i, row):
        result = self.filter(i, row)

        if not result:
            self.emit('filter.row', (i, row))

        return result

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        if self.output_file is not None:
            self.output_file.close()
            self.output_file = None

    def __repr__(self):
        return '<{name} path={path!r} can_resume={can_resume!r}>'.format(
            name=self.__class__.__name__,
            path=self.path,
            can_resume=self.can_resume()
        )

    def already_done_count(self):
        raise NotImplementedError


class LineCountResumer(Resumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.line_count = 0

    def get_insights_from_output(self, enricher):
        self.line_count = 0

        with self.open(mode='r') as f:
            reader = Reader(f)

            count = 0

            for row in reader:
                self.emit('output.row', row)
                count += 1

        self.line_count = count

    def filter(self, i, row):
        if i < self.line_count:
            return False

        return True

    def already_done_count(self):
        return self.line_count


class ThreadSafeResumer(Resumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.already_done = ContiguousRangeSet()

    def get_insights_from_output(self, enricher):
        self.already_done = ContiguousRangeSet()

        with self.open(mode='r') as f:
            reader = Reader(f)

            pos = reader.pos.get(enricher.index_column)

            if pos is None:
                raise MissingColumnError(enricher.index_column)

            for row in reader:
                self.emit('output.row', row)

                try:
                    current_index = int(row[pos])
                except ValueError:
                    raise CorruptedIndexColumn

                self.already_done.add(current_index)

    def filter(self, i, row):
        return not self.already_done.stateful_contains(i)

    def already_done_count(self):
        return len(self.already_done)


class BatchResumer(Resumer):
    def __init__(self, path, value_column, **kwargs):
        super().__init__(path, **kwargs)
        self.last_batch = None
        self.value_column = value_column
        self.value_pos = None
        self.next_cursor = None
        self.values_to_skip = None

    def get_insights_from_output(self, enricher):
        self.last_batch = ReverseReader.last_batch(
            self.path,
            batch_value=self.value_column,
            batch_cursor=enricher.cursor_column,
            end_symbol=enricher.end_symbol
        )
        self.value_pos = enricher.output_pos[self.value_column]

    def filter(self, i, row):
        last_batch = self.last_batch

        if last_batch is None:
            return True

        value = row[self.value_pos]

        # We haven't reached our batch yet
        if value != last_batch.value:
            return False

        # Last batch was completely finished
        elif last_batch.finished:
            self.last_batch = None
            return False

        # Here we need to record additional information
        self.next_cursor = last_batch.cursor
        self.values_to_skip = set(row[self.value_pos] for row in last_batch.rows)

        self.last_batch = None

        return True
