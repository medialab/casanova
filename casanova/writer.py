# =============================================================================
# Casanova Writer
# =============================================================================
#
# A CSV writer that is only really useful if you intend to resume its operation
# somehow
#
import csv

from casanova.resuming import Resumer, LastCellResumer
from casanova.reader import Headers


class Writer(object):
    __supported_resumers__ = (LastCellResumer,)

    def __init__(self, output_file, fieldnames):
        self.fieldnames = fieldnames
        self.headers = Headers(fieldnames)

        can_resume = False

        if isinstance(output_file, Resumer):
            resumer = output_file

            if not isinstance(output_file, self.__class__.__supported_resumers__):
                raise TypeError('%s: does not support %s!' % (self.__class__.__name__, output_file.__class__.__name__))

            can_resume = resumer.can_resume()

            if can_resume:
                resumer.get_insights_from_output(self)

            output_file = resumer.open_output_file()

        self.writer = csv.writer(output_file)

        if not can_resume:
            self.writeheader()

    def writeheader(self):
        self.writer.writerow(self.fieldnames)

    def writerow(self, row):
        self.writer.writerow(row)
