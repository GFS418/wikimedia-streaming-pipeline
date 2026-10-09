"""Create or replace the Unity Catalog views in sql/views/ (idempotent).

uv run python scripts/apply_views.py
"""

from pathlib import Path

from wikistream.dbsql import query

if __name__ == "__main__":
    for f in sorted((Path(__file__).resolve().parents[1] / "sql" / "views").glob("*.sql")):
        query(f.read_text())
        print(f"applied {f.name}")
