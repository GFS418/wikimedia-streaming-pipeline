from wikistream.audit import Seen, reconcile
from wikistream.contract import UpstreamPosition

EQ = "eqiad.mediawiki.recentchange"


def pos(offset, topic=EQ, partition=0):
    return UpstreamPosition(topic, partition, offset)


def main(offset, eid=None):
    return Seen("main", pos(offset), eid or f"id-{offset}")


def test_contiguous_main_only_is_reconciled():
    r = reconcile(main(o) for o in range(100, 110))
    assert r["verdict"] == "reconciled"
    assert r["upstream"][f"{EQ}[0]"]["expected"] == r["upstream"][f"{EQ}[0]"]["accounted"] == 10


def test_canary_gap_is_closed_by_the_drops_ledger():
    # The exact situation from the first live run: 6570336730 was a canary.
    without = [main(6570336729), main(6570336731)]
    assert reconcile(without)["verdict"] == "unexplained_gaps"
    assert reconcile(without)["upstream"][f"{EQ}[0]"]["gap_runs"] == [(6570336730, 6570336730)]
    with_ledger = [*without, Seen("drops", pos(6570336730))]
    r = reconcile(with_ledger)
    assert r["verdict"] == "reconciled"
    assert r["upstream"][f"{EQ}[0]"]["by_source"] == {"main": 2, "drops": 1}


def test_dead_letters_with_positions_close_gaps():
    r = reconcile([main(1), Seen("dlq", pos(2)), main(3)])
    assert r["verdict"] == "reconciled"


def test_gap_explainable_only_by_unattributed_dead_letters():
    r = reconcile([main(1), main(3), Seen("dlq", None)])
    assert r["verdict"] == "explainable_by_unattributed_dlq"
    assert r["unattributed"] == {"dlq": 1}


def test_more_gaps_than_unattributed_dead_letters_is_unexplained():
    r = reconcile([main(1), main(5), Seen("dlq", None)])
    assert r["verdict"] == "unexplained_gaps" and r["missing_upstream_offsets"] == 3


def test_duplicates_are_counted_by_offset_and_by_event_id():
    r = reconcile([main(1), main(2), main(2), main(3)])
    assert r["main_duplicate_event_ids"] == 1
    assert r["upstream"][f"{EQ}[0]"]["repeated_offsets"] == 1
    assert r["verdict"] == "reconciled"  # duplicates are not gaps; reported separately


def test_same_offset_in_two_sources_is_a_conflict():
    r = reconcile([main(1), Seen("dlq", pos(1)), main(2)])
    assert r["upstream"][f"{EQ}[0]"]["cross_source_conflicts"] == 1


def test_upstream_topics_are_reconciled_independently():
    codfw = "codfw.mediawiki.recentchange"
    r = reconcile([main(10), main(11), Seen("drops", pos(5000, topic=codfw))])
    assert set(r["upstream"]) == {f"{EQ}[0]", f"{codfw}[0]"}
    assert r["verdict"] == "reconciled"
