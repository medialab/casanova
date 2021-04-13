# =============================================================================
# Casanova Monkey Enricher
# =============================================================================
#
# A Casanova enricher relying on csvmonkey for performance.
#
from casanova_monkey.reader import MonkeyReader
from casanova.enricher import make_enricher

ThreadsafeMonkeyEnricher, MonkeyEnricher = make_enricher(
    'MonkeyEnricher',
    'casanova_monkey.enricher',
    MonkeyReader
)
