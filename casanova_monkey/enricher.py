# =============================================================================
# Casanova Monkey Enricher
# =============================================================================
#
# A Casanova enricher relying on csvmonkey for performance.
#
from casanova_monkey.reader import CasanovaMonkeyReader
from casanova.enricher import make_enricher

ThreadsafeCasanovaMonkeyEnricher, CasanovaMonkeyEnricher = make_enricher(
    'CasanovaMonkeyEnricher',
    'casanova_monkey.enricher',
    CasanovaMonkeyReader
)
