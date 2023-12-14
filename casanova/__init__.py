# =============================================================================
# Casanova Library Endpoint
# =============================================================================
#
from casanova.contiguous_range_set import ContiguousRangeSet
from casanova.defaults import set_defaults, temporary_defaults
from casanova.enricher import (
    Enricher,
    IndexedEnricher,
    BatchEnricher,
    InferringEnricher,
)
from casanova.headers import Headers, RowWrapper
from casanova.record import (
    TabularRecord,
    tabular_field,
    tabular_fields,
    is_tabular_record_class,
)
from casanova.ndjson import TabularJSONEncoder
from casanova.reader import Reader, Multiplexer
from casanova.resumers import (
    Resumer,
    BasicResumer,
    RowCountResumer,
    IndexedResumer,
    BatchResumer,
    LastCellResumer,
    LastCellComparisonResumer,
)
from casanova.reverse_reader import ReverseReader, Batch
from casanova.serialization import CSVSerializer
from casanova.utils import CsvCellIO, CsvRowIO, CsvIO
from casanova.writer import Writer, InferringWriter

headers = Headers
reader = Reader
enricher = Enricher
indexed_enricher = IndexedEnricher
batch_enricher = BatchEnricher
inferring_enricher = InferringEnricher
reverse_reader = ReverseReader
writer = Writer
inferring_writer = InferringWriter

# Re-exporting statics
count = reader.count
last_cell = reverse_reader.last_cell
last_batch = reverse_reader.last_batch
