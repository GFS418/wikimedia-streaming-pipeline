"""Reconcile main + DLQ + drops topics against upstream offsets. See wikistream/audit.py.

uv run python scripts/audit_topic.py [--since 2026-09-30T23:30:00Z]
"""

from wikistream.audit import main

if __name__ == "__main__":
    main()
