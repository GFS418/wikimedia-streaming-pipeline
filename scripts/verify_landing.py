"""Verify landed files against Kafka. Read-only. See wikistream/verify_landing.py.

uv run python scripts/verify_landing.py
"""

from wikistream.verify_landing import main

if __name__ == "__main__":
    main()
