"""Write pipeline/wikistream_lake/landing_schema.py from the Avro contract.

uv run python scripts/generate_landing_schema.py
"""

from pathlib import Path

from wikistream.lake_schema import render_module

TARGET = Path(__file__).resolve().parents[1] / "pipeline" / "wikistream_lake" / "landing_schema.py"

if __name__ == "__main__":
    TARGET.write_text(render_module())
    print(f"wrote {TARGET}")
