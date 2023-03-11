# =============================================================================
# Casanova Writer
# =============================================================================
#
# A CSV writer that is only really useful if you intend to resume its operation
# somehow
#
import csv

from casanova.defaults import DEFAULTS
from casanova.resumers import Resumer, LastCellResumer
from casanova.namedrecord import coerce_row, coerce_fieldnames
from casanova.reader import Headers
from casanova.utils import py310_wrap_csv_writerow, strip_null_bytes_from_row


class Writer(object):
    __supported_resumers__ = (LastCellResumer,)

    def __init__(
        self,
        output_file,
        fieldnames=None,
        strip_null_bytes_on_write=None,
        dialect=None,
        delimiter=None,
        quotechar=None,
        quoting=None,
        escapechar=None,
        lineterminator=None,
        write_header=True,
    ):
        if strip_null_bytes_on_write is None:
            strip_null_bytes_on_write = DEFAULTS.strip_null_bytes_on_write

        if not isinstance(strip_null_bytes_on_write, bool):
            raise TypeError('expecting a boolean as "strip_null_bytes_on_write" kwarg')

        self.strip_null_bytes_on_write = strip_null_bytes_on_write

        fieldnames = coerce_fieldnames(fieldnames)

        self.fieldnames = fieldnames
        self.headers = Headers(fieldnames) if fieldnames is not None else None
        self.no_headers = fieldnames is None

        can_resume = False

        if isinstance(output_file, Resumer):
            if self.fieldnames is None:
                raise NotImplementedError

            resumer = output_file

            if not isinstance(output_file, self.__class__.__supported_resumers__):
                raise TypeError(
                    "%s: does not support %s!"
                    % (self.__class__.__name__, output_file.__class__.__name__)
                )

            can_resume = resumer.can_resume()

            if can_resume:
                # NOTE: how about null bytes
                resumer.get_insights_from_output(
                    self,
                    no_headers=self.no_headers,
                    dialect=dialect,
                    quotechar=quotechar,
                    delimiter=delimiter,
                )

            output_file = resumer.open_output_file()

        # Instantiating writer
        self.dialect = dialect
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.escapechar = escapechar
        self.quoting = quoting
        self.lineterminator = lineterminator

        writer_kwargs = {}

        if dialect is not None:
            writer_kwargs["dialect"] = dialect

        if delimiter is not None:
            writer_kwargs["delimiter"] = delimiter

        if quotechar is not None:
            writer_kwargs["quotechar"] = quotechar

        if escapechar is not None:
            writer_kwargs["escapechar"] = escapechar

        if quoting is not None:
            writer_kwargs["quoting"] = quoting

        if lineterminator is not None:
            writer_kwargs["lineterminator"] = lineterminator

        self.__writer = csv.writer(output_file, **writer_kwargs)

        if not strip_null_bytes_on_write:
            self.__writerow = py310_wrap_csv_writerow(self.__writer)
        else:
            self.__writerow = lambda row: self.__writer.writerow(
                strip_null_bytes_from_row(row)
            )

        self.should_write_header = not can_resume and self.fieldnames is not None

        if self.should_write_header and write_header:
            self.writeheader()

    def writerow(self, row):
        row = coerce_row(row)
        self.__writerow(row)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

    def writeheader(self):
        if self.fieldnames is None:
            raise TypeError("cannot write header if fieldnames were not provided")

        self.should_write_header = False

        row = self.fieldnames

        if self.strip_null_bytes_on_write:
            row = strip_null_bytes_from_row(row)

        self.writerow(row)
