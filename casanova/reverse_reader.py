# =============================================================================
# Casanova Reverse Reader
# =============================================================================
#
# A reader reading the file backwards in order to read last lines in constant
# time. This is sometimes useful to be able to resume some computations
# where they were left off.
#
from casanova.reader import Reader
from casanova.exceptions import MissingColumnError


class Batch(object):
    __slots__ = ("value", "finished", "cursor", "rows")

    def __init__(self, value, finished=False, cursor=None, rows=None):
        self.value = value
        self.finished = finished
        self.cursor = cursor
        self.rows = rows or []

    def __eq__(self, other):
        return (
            self.value == other.value
            and self.finished == other.finished
            and self.cursor == other.cursor
            and self.rows == other.rows
        )

    def __iter__(self):
        return iter(self.rows)

    def collect(self, pos):
        return set(row[pos] for row in self)

    def __repr__(self):
        class_name = self.__class__.__name__

        return (
            "<%(class_name)s value=%(value)s finished=%(finished)s cursor=%(cursor)s rows=%(rows)i>"
        ) % {
            "class_name": class_name,
            "value": self.value,
            "finished": self.finished,
            "cursor": self.cursor,
            "rows": len(self.rows),
        }


class ReverseReader(Reader):
    namespace = "casanova.reverse_reader"

    def __init__(self, input_file, **kwargs):
        super().__init__(input_file, reverse=True, **kwargs)

    @staticmethod
    def last_cell(input_file, column, **kwargs):
        with ReverseReader(input_file, **kwargs) as reader:
            if reader.empty:
                return None

            return next(reader.cells(column))

    @staticmethod
    def last_batch(input_file, batch_value, batch_cursor, end_symbol, **kwargs):
        with ReverseReader(input_file, **kwargs) as reader:
            if reader.empty:
                return None

            batch = None

            if batch_value not in reader.headers:
                raise MissingColumnError(batch_value)

            if batch_cursor not in reader.headers:
                raise MissingColumnError(batch_cursor)

            batch_value_pos = reader.headers[batch_value]
            batch_cursor_pos = reader.headers[batch_cursor]

            for row in reader:
                value = row[batch_value_pos]
                cursor = row[batch_cursor_pos]

                if batch is None:
                    batch = Batch(value)

                if value != batch.value:
                    return batch

                if cursor == end_symbol:
                    batch.finished = True
                    return batch

                if cursor:
                    batch.cursor = cursor
                    return batch

                batch.rows.append(row)

            return batch
