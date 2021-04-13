# =============================================================================
#  Monkey Library Endpoint
# =============================================================================
#
from casanova_monkey.enricher import (
    MonkeyEnricher,
    ThreadsafeMonkeyEnricher,
    BatchMonkeyEnricher
)
from casanova_monkey.reader import (
    MonkeyReader
)

enricher = MonkeyEnricher
threadsafe_enricher = ThreadsafeMonkeyEnricher
batch_enricher = BatchMonkeyEnricher
reader = MonkeyReader
