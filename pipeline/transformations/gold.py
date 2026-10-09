"""Gold: per-wiki, per-minute activity from silver's change feed (decisions D17, D24).

silver_recentchange is an AUTO CDC target, so it is read through its change feed (the
documented way to stream from one). With first-copy-wins sequencing (D17) a later duplicate
never updates a row, so the feed carries only inserts: each event counted exactly once.
Windows are emitted once the 20-minute watermark passes them (append semantics).

If silver is ever fully refreshed, refresh gold too: a new change-feed stream starts from
silver's current snapshot.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from wikistream_lake.gold import WATERMARK, eligible, per_wiki_minute


@dp.table(
    name="gold_wiki_minute",
    comment="Events per wiki per minute of event time: edits, new pages, log, categorize, "
    "other; bot vs human changes; bytes added/removed. Watermark 20 min (decision D24).",
    table_properties={"quality": "gold"},
)
def gold_wiki_minute():
    inserts = (
        spark.readStream.option("readChangeFeed", "true")
        .table("silver_recentchange")
        .where(F.col("_change_type") == "insert")
    )
    return per_wiki_minute(eligible(inserts).withWatermark("event_time", WATERMARK))
