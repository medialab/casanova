# =============================================================================
# Casanova Library Endpoint
# =============================================================================
#
from casanova.contiguous_range_set import ContiguousRangeSet
from casanova.defaults import set_defaults, temporary_defaults
from casanova.enricher import Enricher, ThreadSafeEnricher, BatchEnricher
from casanova.headers import Headers, RowWrapper
from casanova.namedrecord import namedrecord, TabularRecord, tabular_field
from casanova.reader import Reader, Multiplexer
from casanova.resumers import (
    Resumer,
    RowCountResumer,
    ThreadSafeResumer,
    BatchResumer,
    LastCellResumer,
    LastCellComparisonResumer,
)
from casanova.reverse_reader import ReverseReader, Batch
from casanova.serialization import CSVSerializer
from casanova.utils import CsvCellIO, CsvRowIO, CsvIO
from casanova.writer import Writer

headers = Headers
reader = Reader
enricher = Enricher
threadsafe_enricher = ThreadSafeEnricher
batch_enricher = BatchEnricher
reverse_reader = ReverseReader
writer = Writer

# Re-exporting statics
count = reader.count
last_cell = reverse_reader.last_cell
last_batch = reverse_reader.last_batch
