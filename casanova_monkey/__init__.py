# =============================================================================
#  Monkey Library Endpoint
# =============================================================================
#
from casanova_monkey.enricher import (
    MonkeyEnricher,
    ThreadsafeMonkeyEnricher
)
from casanova_monkey.reader import (
    MonkeyReader
)

enricher = MonkeyEnricher
threadsafe_enricher = ThreadsafeMonkeyEnricher
reader = MonkeyReader
