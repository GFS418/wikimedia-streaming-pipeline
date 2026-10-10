"""Monitor alert rules (Phase 4): healthy snapshots are quiet; each fault raises its alert."""

from wikistream.monitor import Thresholds, evaluate

N = 5000
HEALTHY = {
    "producer_freshness_s": 0.4,
    "upstream_lag_s": 1.2,
    "unlanded": {0: 1200, 1: 4999, 2: 0},
    "landing_freshness_s": 13 * 60,  # an overnight fill
    "dlq_total": 3,
}


def alerts(**changes):
    return evaluate(HEALTHY | changes, Thresholds(), N, prev_dlq=3)


def test_healthy_snapshot_raises_nothing():
    assert alerts() == []


def test_each_fault_raises_its_own_alert():
    assert [a.split(":")[0] for a in alerts(producer_freshness_s=121)] == ["producer_stalled"]
    assert [a.split(":")[0] for a in alerts(upstream_lag_s=301)] == ["upstream_lag"]
    assert [a.split(":")[0] for a in alerts(unlanded={0: 10_000, 1: 0, 2: 0})] == [
        "landing_backlog"
    ]
    assert [a.split(":")[0] for a in alerts(landing_freshness_s=31 * 60)] == ["landing_stalled"]
    assert [a.split(":")[0] for a in alerts(dlq_total=4)] == ["dead_letters"]


def test_backlog_below_two_files_per_partition_is_not_an_alert():
    assert alerts(unlanded={0: 9_999, 1: 9_999, 2: 9_999}) == []


def test_first_tick_has_no_dead_letter_baseline():
    assert evaluate(HEALTHY | {"dlq_total": 99}, Thresholds(), N, prev_dlq=None) == []


def test_unknown_values_do_not_alert():
    quiet = alerts(producer_freshness_s=None, upstream_lag_s=None, landing_freshness_s=None)
    assert quiet == []
