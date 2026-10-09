"""Run SQL on the Databricks SQL warehouse through the Databricks CLI.

The CLI handles authentication (OAuth profile), so no token ever passes through this code.
Uses the Statement Execution API with inline JSON results; fine for small result sets
(verification queries), not for bulk export.
"""

from __future__ import annotations

import json
import os
import subprocess
import time

WAREHOUSE_ID = os.environ.get("WIKISTREAM_WAREHOUSE_ID", "2fbe4643eb2ff320")


def _api(method: str, path: str, body: dict | None = None) -> dict:
    cmd = ["databricks", "api", method, path]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    out = subprocess.run(cmd, capture_output=True, text=True, check=True).stdout
    return json.loads(out)


def query(sql: str, warehouse_id: str = WAREHOUSE_ID, timeout_s: float = 300) -> list[dict]:
    resp = _api(
        "post",
        "/api/2.0/sql/statements",
        {
            "statement": sql,
            "warehouse_id": warehouse_id,
            "wait_timeout": "50s",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        },
    )
    deadline = time.monotonic() + timeout_s
    while resp["status"]["state"] in ("PENDING", "RUNNING"):
        if time.monotonic() > deadline:
            raise TimeoutError(f"statement {resp['statement_id']} still {resp['status']['state']}")
        time.sleep(2)
        resp = _api("get", f"/api/2.0/sql/statements/{resp['statement_id']}")
    if resp["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(f"SQL failed: {resp['status']}")
    columns = resp.get("manifest", {}).get("schema", {}).get("columns")
    if not columns:  # DDL and other statements without a result set
        return []
    cols = [c["name"] for c in columns]
    rows = resp.get("result", {}).get("data_array", []) or []
    if resp["manifest"].get("total_chunk_count", 1) > 1:
        raise RuntimeError("result spans several chunks; narrow the query")
    return [dict(zip(cols, r, strict=True)) for r in rows]
