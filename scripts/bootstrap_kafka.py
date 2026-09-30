"""Create topics, set FULL_TRANSITIVE, register the v1 value schema. Idempotent.

uv run python scripts/bootstrap_kafka.py
"""

import json

from wikistream.admin import bootstrap

if __name__ == "__main__":
    print(json.dumps(bootstrap(), indent=2))
