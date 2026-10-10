"""Re-fetch named upstream events from Wikimedia's history and produce them. See
wikistream/backfill.py. Stop the producer first.

uv run python scripts/backfill_upstream.py \\
    'eqiad.mediawiki.recentchange[0]@6598017959=2026-10-10T04:06:56.735Z'
"""

from wikistream.backfill import main

if __name__ == "__main__":
    main()
