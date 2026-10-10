"""Deterministic crash points for failure drills (Phase 4). Inert unless configured.

    WIKISTREAM_CRASH_AT=<point>:<n>

hard-exits the process the n-th time execution reaches <point>, with os._exit(137): no
finally blocks, no atexit, no flush, no clean consumer-group leave, the same effect on the
process as SIGKILL. Points (where the code calls crash_point):

    consumer.before_put     a range is complete in memory but not yet uploaded
    consumer.after_put      a range is uploaded; the next one not started
    consumer.mid_range      the n-th row has just been buffered
    producer.after_produce  the n-th message is queued in librdkafka, unflushed

Random kills from outside the process (scripts/drill.py hard_kill: SIGKILL via the
container's init, so Docker sees a crash) complement these: crash points give reproducible
timing, random kills give realism.
"""

from __future__ import annotations

import os
import sys

ENV = "WIKISTREAM_CRASH_AT"
EXIT_CODE = 137  # what a SIGKILLed process reports
_hits: dict[str, int] = {}


def _target() -> tuple[str, int] | None:
    spec = os.environ.get(ENV, "").strip()
    if not spec:
        return None
    point, _, n = spec.partition(":")
    return point, int(n or 1)


def crash_point(name: str) -> None:
    target = _target()
    if target is None or target[0] != name:
        return
    _hits[name] = _hits.get(name, 0) + 1
    if _hits[name] == target[1]:
        sys.stderr.write(f"CRASH POINT {name} #{target[1]}: os._exit({EXIT_CODE})\n")
        sys.stderr.flush()
        os._exit(EXIT_CODE)
