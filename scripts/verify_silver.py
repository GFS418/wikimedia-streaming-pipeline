"""Reconcile silver and quarantine against bronze (decisions D17, D18).

Every bronze row must be accounted for exactly once:
    bronze rows = silver rows + quarantined rows + later duplicates dropped by AUTO CDC
with exactly one silver row per meta_id, no meta_id in both silver and quarantine, and,
where duplicates exist, silver holding the earliest copy (first copy wins). Also prints
the per-rule expectation metrics the pipeline recorded for silver_checked.

    uv run python scripts/verify_silver.py
"""

import json

from wikistream.dbsql import query

S = "workspace.wikistream"


def main() -> None:
    c = query(f"""
        WITH b AS (
          SELECT value.meta.id AS meta_id, kafka.partition AS p, kafka.offset AS o
          FROM {S}.bronze_recentchange),
        q AS (SELECT meta_id FROM {S}.silver_quarantine),
        valid AS (SELECT * FROM b WHERE meta_id NOT IN (SELECT meta_id FROM q)),
        firsts AS (SELECT meta_id, min(struct(p, o)) AS first FROM valid GROUP BY meta_id)
        SELECT
          (SELECT count(*) FROM b) AS bronze_rows,
          (SELECT count(DISTINCT meta_id) FROM b) AS bronze_distinct_ids,
          (SELECT count(*) FROM {S}.silver_recentchange) AS silver_rows,
          (SELECT count(DISTINCT meta_id) FROM {S}.silver_recentchange) AS silver_distinct_ids,
          (SELECT count(*) FROM q) AS quarantine_rows,
          (SELECT count(*) FROM valid) - (SELECT count(*) FROM firsts) AS duplicates_dropped,
          (SELECT count(*) FROM {S}.silver_recentchange s JOIN q USING (meta_id))
            AS ids_in_both,
          (SELECT count(*) FROM {S}.silver_recentchange s JOIN firsts f USING (meta_id)
            WHERE struct(s.kafka_partition, s.kafka_offset) <> f.first) AS not_first_copy
        """)[0]
    c = {k: int(v) for k, v in c.items()}
    flags = query(f"""
        SELECT flag, count(*) AS n FROM {S}.silver_recentchange
        LATERAL VIEW explode(dq_flags) t AS flag GROUP BY flag""")
    rules = query(f"""
        SELECT r.failed_rule AS rule, count(*) AS n FROM {S}.silver_quarantine
        LATERAL VIEW explode(failed_rules) r AS failed_rule GROUP BY r.failed_rule""")
    expectations = query(f"""
        SELECT e.name AS rule, sum(e.passed_records) AS passed, sum(e.failed_records) AS failed
        FROM (
          SELECT explode(from_json(
            details:flow_progress:data_quality:expectations,
            'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>'
          )) AS e
          FROM event_log(TABLE({S}.bronze_recentchange))
          WHERE event_type = 'flow_progress'
            AND details:flow_progress:data_quality:expectations IS NOT NULL
            -- Only since the last full refresh (RESETTING): earlier updates' rows were
            -- recomputed, so summing all history would count them twice.
            AND timestamp >= coalesce(
              (SELECT max(timestamp) FROM event_log(TABLE({S}.bronze_recentchange))
               WHERE event_type = 'update_progress'
                 AND details:update_progress:state = 'RESETTING'),
              TIMESTAMP '1970-01-01'))
        GROUP BY e.name ORDER BY e.name""")

    accounted = c["silver_rows"] + c["quarantine_rows"] + c["duplicates_dropped"]
    failures = []
    if accounted != c["bronze_rows"]:
        failures.append(
            f"silver + quarantine + duplicates = {accounted} != bronze {c['bronze_rows']}"
        )
    if c["silver_rows"] != c["silver_distinct_ids"]:
        failures.append("silver has more than one row for some meta_id")
    if c["ids_in_both"]:
        failures.append(f"{c['ids_in_both']} meta_ids in both silver and quarantine")
    if c["not_first_copy"]:
        failures.append(f"{c['not_first_copy']} silver rows are not the earliest copy")
    report = {
        "counts": c,
        "silver_dq_flags": {r["flag"]: int(r["n"]) for r in flags},
        "quarantine_by_rule": {r["rule"]: int(r["n"]) for r in rules},
        "expectation_metrics": {
            r["rule"]: {"passed": int(r["passed"]), "failed": int(r["failed"])}
            for r in expectations
        },
        "failures": failures,
        "verdict": "ok" if not failures else "FAILED",
    }
    print(json.dumps(report, indent=2))
    raise SystemExit(0 if not failures else 1)


if __name__ == "__main__":
    main()
