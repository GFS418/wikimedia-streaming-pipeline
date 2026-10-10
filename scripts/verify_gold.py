"""Reconcile gold against silver (decision D24). Read-only.

For every window gold has finalized (window_end at or before gold's newest window_end), the
ten gold metrics per (window, wiki) must equal the same aggregation computed directly from
silver (historical rows excluded). Any shortfall is a row the watermark dropped as late.
Rows repaired in later by scripts/backfill_upstream.py (Kafka header `backfill`) arrive
hours after their minute closed, so gold drops them by design: they are left out of the
comparison and reported on their own. Also checks gold's own identities and key uniqueness,
and that the bot-share view agrees.

    uv run python scripts/verify_gold.py
"""

import json

from wikistream.dbsql import query

S = "workspace.wikistream"
M = [
    "events",
    "edits",
    "new_pages",
    "log_events",
    "categorize_events",
    "other_events",
    "bot_changes",
    "human_changes",
    "bytes_added",
    "bytes_removed",
]

SILVER_AGG = f"""
  SELECT window(event_time, '1 minute').start AS window_start, wiki,
    count(*) AS events,
    count_if(type = 'edit') AS edits,
    count_if(type = 'new') AS new_pages,
    count_if(type = 'log') AS log_events,
    count_if(type = 'categorize') AS categorize_events,
    count_if(type NOT IN ('edit', 'new', 'log', 'categorize')) AS other_events,
    count_if(type IN ('edit', 'new') AND bot) AS bot_changes,
    count_if(type IN ('edit', 'new') AND NOT bot) AS human_changes,
    sum(CASE WHEN type IN ('edit', 'new')
        THEN greatest(coalesce(length_new, 0) - coalesce(length_old, 0), 0) ELSE 0 END)
      AS bytes_added,
    sum(CASE WHEN type IN ('edit', 'new')
        THEN greatest(coalesce(length_old, 0) - coalesce(length_new, 0), 0) ELSE 0 END)
      AS bytes_removed
  FROM {S}.silver_recentchange
  WHERE NOT array_contains(dq_flags, 'historical_event_time')
    AND NOT map_contains_key(kafka_headers, 'backfill')
  GROUP BY 1, 2"""


def main() -> None:
    s_cols = ", ".join(f"s.{m} AS s_{m}" for m in M)
    g_cols = ", ".join(f"g.{m} AS g_{m}" for m in M)
    diffs = ",\n".join(f"sum(abs(coalesce(s_{m}, 0) - coalesce(g_{m}, 0))) AS diff_{m}" for m in M)
    r = query(f"""
      WITH s AS ({SILVER_AGG}),
           g AS (SELECT * FROM {S}.gold_wiki_minute),
           frontier AS (SELECT max(window_end) AS f FROM g),
           j AS (SELECT coalesce(s.window_start, g.window_start) AS window_start, {s_cols}, {g_cols}
                 FROM s FULL OUTER JOIN g ON s.window_start = g.window_start AND s.wiki = g.wiki)
      SELECT
        (SELECT f FROM frontier) AS gold_frontier,
        (SELECT min(window_start) FROM g) AS gold_first_window,
        count(*) AS window_wiki_pairs,
        count_if(g_events IS NULL) AS pairs_missing_in_gold,
        count_if(s_events IS NULL) AS pairs_only_in_gold,
        sum(coalesce(s_events, 0)) AS silver_events_finalized,
        sum(coalesce(g_events, 0)) AS gold_events_finalized,
        {diffs}
      FROM j
      WHERE window_start + INTERVAL 1 MINUTE <= (SELECT f FROM frontier)""")[0]
    ident = query(f"""
      SELECT count(*) AS gold_rows,
        count(*) - count(DISTINCT window_start, wiki) AS duplicate_keys,
        count_if(events <> edits + new_pages + log_events + categorize_events + other_events)
          AS events_identity_broken,
        count_if(bot_changes + human_changes <> edits + new_pages) AS change_identity_broken
      FROM {S}.gold_wiki_minute""")[0]
    view = query(f"""
      SELECT count(*) AS minutes,
        count_if(abs(v.bot_changes - g.bot) > 0) AS minutes_disagreeing,
        round(sum(v.bot_changes) / sum(v.bot_changes + v.human_changes), 4) AS overall_bot_share
      FROM {S}.gold_bot_share_minute v
      JOIN (SELECT window_start, sum(bot_changes) AS bot FROM {S}.gold_wiki_minute
            GROUP BY 1) g USING (window_start)""")[0]

    backfilled = query(f"""
      SELECT count(*) AS rows, min(event_time) AS first_event_time,
        max(event_time) AS last_event_time
      FROM {S}.silver_recentchange WHERE map_contains_key(kafka_headers, 'backfill')""")[0]

    report = {
        "reconciliation": r,
        "gold_identities": ident,
        "bot_share_view": view,
        "backfilled_rows_late_by_design": backfilled,
    }
    failures = [k for k, v in r.items() if k.startswith("diff_") and int(v or 0) != 0]
    failures += [k for k in ("pairs_missing_in_gold", "pairs_only_in_gold") if int(r[k])]
    failures += [k for k, v in ident.items() if k != "gold_rows" and int(v)]
    failures += ["bot_share_view"] if int(view["minutes_disagreeing"]) else []
    report["failures"] = failures
    report["verdict"] = "ok" if not failures else "FAILED"
    print(json.dumps(report, indent=2))
    raise SystemExit(0 if not failures else 1)


if __name__ == "__main__":
    main()
