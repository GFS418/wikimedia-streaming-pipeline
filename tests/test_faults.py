"""Phase 4 crash points: inert by default, and a hard exit at exactly the n-th hit."""

import os
import subprocess
import sys

import pytest

SCRIPT = """
import sys
from wikistream.faults import crash_point
for i in range(1, 6):
    crash_point("consumer.before_put")
    print(f"survived {i}", flush=True)
try:
    pass
finally:
    print("finally ran", flush=True)
"""


def run(env_value):
    env = {k: v for k, v in os.environ.items() if k != "WIKISTREAM_CRASH_AT"}
    if env_value is not None:
        env["WIKISTREAM_CRASH_AT"] = env_value
    return subprocess.run([sys.executable, "-c", SCRIPT], env=env, capture_output=True, text=True)


def test_inert_without_configuration():
    p = run(None)
    assert p.returncode == 0
    assert p.stdout.splitlines()[-2:] == ["survived 5", "finally ran"]


def test_other_points_are_ignored():
    assert run("producer.after_produce:1").returncode == 0


@pytest.mark.parametrize("n", [1, 3])
def test_hard_exit_at_the_nth_hit_like_sigkill(n):
    p = run(f"consumer.before_put:{n}")
    assert p.returncode == 137
    assert p.stdout.splitlines() == [f"survived {i}" for i in range(1, n)]  # nothing after
    assert "finally ran" not in p.stdout  # os._exit skips cleanup, as SIGKILL does
    assert f"CRASH POINT consumer.before_put #{n}" in p.stderr


def test_count_defaults_to_one():
    assert run("consumer.before_put").returncode == 137
