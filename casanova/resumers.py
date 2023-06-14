# =============================================================================
# Casanova Resuming Strategies
# =============================================================================
#
# A collection of process resuming strategies acknowledged by casanova
# enrichers.
#
from typing import Set, Optional

from threading import Lock
from os.path import isfile, getsize
from dataclasses import dataclass

from casanova.reader import Reader
from casanova.reverse_reader import ReverseReader
from casanova.exceptions import (
    ResumeError,
    NotResumableError,
    MissingColumnError,
    CorruptedIndexColumnError,
)
from casanova.contiguous_range_set import ContiguousRangeSet


class Resumer(object):
    def __init__(self, path, listener=None, encoding="utf-8"):
        self.path = path
        self.encoding = encoding
        self.output_file = None
        self.lock = Lock()
        self.popped = False

        self.listener = None

        if listener is not None:
            self.set_listener(listener)

    def set_listener(self, listener):
        if not callable(listener):
            raise TypeError("listener should be callable")

        self.listener = listener

    def can_resume(self):
        return isfile(self.path) and getsize(self.path) > 0

    def open(self, mode="a", newline=""):
        return open(self.path, mode=mode, encoding=self.encoding, newline=newline)

    def open_output_file(self, **kwargs):
        if self.output_file is not None:
            raise ResumeError("output file is already opened")

        mode = "a+" if self.can_resume() else "w"

        self.output_file = self.open(mode=mode, **kwargs)
        return self.output_file

    def emit(self, event, payload):
        if self.listener is None:
            return

        with self.lock:
            self.listener(event, payload)

    def get_insights_from_output(self, enricher, **reader_kwargs):
        raise NotImplementedError

    def filter_row(self, i, row):
        result = self.filter(i, row)

        if not result:
            self.emit("input.row.filter", row)

        return result

    def get_state(self):
        raise NotImplementedError

    def pop_state(self):
        if not self.popped:
            self.popped = True
            return self.get_state()

        return None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def flush(self) -> None:
        if self.output_file is not None:
            self.output_file.flush()

    def close(self):
        if self.output_file is not None:
            self.output_file.close()
            self.output_file = None

    def __repr__(self):
        return "<{name} path={path!r} can_resume={can_resume!r}>".format(
            name=self.__class__.__name__, path=self.path, can_resume=self.can_resume()
        )

    def already_done_count(self):
        raise NotImplementedError


class BasicResumer(Resumer):
    def get_insights_from_output(self, enricher, **reader_kwargs):
        return None


class RowCountResumer(Resumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.row_count = 0

    def get_insights_from_output(self, enricher, **reader_kwargs):
        self.row_count = 0

        with self.open(mode="r") as f:
            reader = Reader(f, **reader_kwargs)

            count = 0

            for row in reader:
                self.emit("output.row.read", row)
                count += 1

        self.row_count = count

    def resume(self, enricher):
        i = 0
        iterator = iter(enricher)

        while i < self.row_count:
            row = next(iterator)
            self.emit("input.row.filter", row)
            i += 1

    def already_done_count(self):
        return self.row_count


class ThreadSafeResumer(Resumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.already_done = ContiguousRangeSet()

    def get_insights_from_output(self, enricher, **reader_kwargs):
        self.already_done = ContiguousRangeSet()

        with self.open(mode="r") as f:
            reader = Reader(f, **reader_kwargs)

            assert reader.headers is not None

            pos = reader.headers.get(enricher.index_column)

            if pos is None:
                raise MissingColumnError(enricher.index_column)

            for row in reader:
                self.emit("output.row.read", row)

                try:
                    current_index = int(row[pos])
                except ValueError:
                    raise CorruptedIndexColumnError

                self.already_done.add(current_index)

    def filter(self, i, row):
        return not self.already_done.stateful_contains(i)

    def already_done_count(self):
        return len(self.already_done)


@dataclass
class BatchResumerContext:
    last_cursor: Optional[str]
    values_to_skip: Optional[Set[str]]


class BatchResumer(Resumer):
    def __init__(self, path, value_column, **kwargs):
        super().__init__(path, **kwargs)
        self.last_batch = None
        self.value_column = value_column
        self.value_pos = None
        self.last_cursor = None
        self.values_to_skip = None
        self.read_count = 0

    def get_insights_from_output(self, enricher, **reader_kwargs):
        self.last_batch = ReverseReader.last_batch(
            self.path,
            batch_value=self.value_column,
            batch_cursor=enricher.cursor_column,
            end_symbol=enricher.end_symbol,
            **reader_kwargs
        )
        self.value_pos = enricher.headers[self.value_column]
        self.last_cursor = None
        self.values_to_skip = None

    def get_state(self):
        return BatchResumerContext(self.last_cursor, self.values_to_skip)

    def already_done_count(self) -> int:
        return self.read_count

    def resume(self, enricher):
        last_batch = self.last_batch

        if last_batch is None:
            return

        while True:
            row = enricher.peek()

            if row is None:
                raise NotResumableError

            self.emit("input.row.filter", row)

            value = row[self.value_pos]

            # We haven't reached our batch yet
            if value != last_batch.value:
                next(enricher)
                self.read_count += 1
                continue

            # Last batch was completely finished
            elif last_batch.finished:
                next(enricher)
                self.read_count += 1
                break

            # Here we need to record additional information
            self.last_cursor = last_batch.cursor
            self.values_to_skip = set(row[self.value_pos] for row in last_batch.rows)

            break


class LastCellResumer(Resumer):
    def __init__(self, path, value_column, **kwargs):
        super().__init__(path, **kwargs)
        self.last_cell = None
        self.value_column = value_column

    def get_insights_from_output(self, enricher, **reader_kwargs):
        self.last_cell = ReverseReader.last_cell(
            self.path, column=self.value_column, **reader_kwargs
        )

    def get_state(self):
        return self.last_cell


class LastCellComparisonResumer(LastCellResumer):
    """
    Warning : this resumer will not work as desired if the column read contains duplicate values.
    """

    def resume(self, enricher):
        for row in enricher:
            self.emit("input.row.filter", row)

            if row[self.value_column] == self.last_cell:
                break
