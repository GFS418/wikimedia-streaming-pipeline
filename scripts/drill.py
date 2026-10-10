"""Phase 4 failure drills: inject one failure, let the system recover unaided, verify.

    uv run python scripts/drill.py crash --point consumer.before_put --n 1 --backlog-s 120
    uv run python scripts/drill.py crash --point consumer.after_put --n 1 --backlog-s 120
    uv run python scripts/drill.py crash --point consumer.mid_range --n 2500
    uv run python scripts/drill.py crash --point producer.after_produce --n 20000 --backlog-s 60
    uv run python scripts/drill.py kill --target kafka
    uv run python scripts/drill.py soak --kills 20 --min-gap 30 --max-gap 150
    uv run python scripts/drill.py pipeline-cancel

crash: stop the service cleanly, optionally let a backlog build, run a one-off copy of it
with WIKISTREAM_CRASH_AT set (src/wikistream/faults.py) until it dies with exit 137, then
restart the normal service. kill: SIGKILL a service's process from outside the process
(hard_kill); the container exits 137 and its restart policy brings it back with no human in
the loop. soak: many random kills. pipeline-cancel: stop a Databricks update mid-run, then
run another.

Recovery means: every complete 5,000-offset range in Kafka has landed (each partition's
unlanded tail is less than one file) and the producer is within 2 minutes of live. Then:
the upstream audit (no missing offset) and the landing check (every file landed since the
drill began: one S3 version, MD5 = ETag, byte-identical when re-derived from Kafka).
Reports go to reports/phase4/drills/.
"""

from __future__ import annotations

import argparse
import json
import random
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

import boto3
from confluent_kafka import Consumer, KafkaException, TopicPartition

from wikistream.audit import collect, reconcile
from wikistream.config import Settings
from wikistream.consumer import last_written_end
from wikistream.cursor import slowest
from wikistream.producer import H_CURSOR
from wikistream.verify_landing import verify as verify_landing

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports" / "phase4" / "drills"
PIPELINE_ID = "f6ba7758-c7bb-4b6e-9e62-a67f3d52ddff"
S = Settings()
SERVICES = ("producer", "consumer", "kafka", "schema-registry")


def now_ms() -> int:
    return int(time.time() * 1000)


def iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def sh(*cmd: str, check: bool = True) -> str:
    p = subprocess.run(cmd, capture_output=True, text=True)
    if check and p.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} -> {p.returncode}: {p.stderr[-800:]}")
    return p.stdout


def compose(*args: str, check: bool = True) -> str:
    return sh("docker", "compose", "--profile", "capture", *args, check=check)


def container(name: str) -> dict:
    out = sh(
        "docker",
        "inspect",
        "-f",
        "{{.State.Status}} {{.State.ExitCode}} {{.RestartCount}}",
        name,
        check=False,
    )
    if not out.strip():
        return {"status": "missing"}
    status, code, restarts = out.split()
    return {"status": status, "exit_code": int(code), "restarts": int(restarts)}


def container_logs(name: str) -> str:
    """Both streams: our logs and the CRASH POINT line go to the container's stderr."""
    p = subprocess.run(["docker", "logs", name], capture_output=True, text=True)
    return p.stdout + p.stderr


def die_codes(name: str, since_s: float) -> list[int]:
    """Exit codes of the container's deaths since `since_s`, from the Docker event log."""
    out = sh(
        "docker", "events", "--since", f"{since_s:.3f}", "--until", f"{time.time():.3f}",
        "--filter", f"container={name}", "--filter", "event=die",
        "--format", "{{.Actor.Attributes.exitCode}}",
    )  # fmt: skip
    return [int(x) for x in out.split()]


def hard_kill(name: str) -> dict:
    """SIGKILL the service's process so that Docker sees a crash, as after an OOM kill.

    Not `docker kill`: Docker records that as an operator stop, and `restart: unless-stopped`
    then deliberately leaves the container down (2026-10-10: Kafka stayed down 10 minutes and
    the drill tested nothing, D30). Under `init: true` the service is a child of tini (PID 1);
    `kill -9 -1` from an exec'd shell reaches every process except PID 1 and the shell, tini
    sees its child die and exits 137, and the restart policy restarts the container."""
    before, t0 = container(name), time.time()
    sh("docker", "exec", name, "sh", "-c", "kill -9 -1", check=False)
    back_after = wait_until(
        f"{name} restarted by its policy",
        lambda: (
            (c := container(name))["restarts"] > before["restarts"] and c["status"] == "running"
        ),
        600,
        1,
    )
    return {"exit_codes": die_codes(name, t0), "back_running_after_s": round(back_after, 1)}


def kafka_highs() -> dict[int, int]:
    c = Consumer({"bootstrap.servers": S.bootstrap_servers, "group.id": "drill"})
    try:
        return {
            p: c.get_watermark_offsets(TopicPartition(S.main_topic.name, p), timeout=10)[1]
            for p in range(S.main_topic.partitions)
        }
    finally:
        c.close()


def landed_ends() -> dict[int, int | None]:
    s3 = boto3.client("s3")
    return {
        p: last_written_end(s3, S.landing_bucket, S.landing_prefix, p)
        for p in range(S.main_topic.partitions)
    }


def eqiad_lag_s() -> float | None:
    """Seconds between now and the eqiad position of the SLOWEST partition tail. After a
    producer crash the fastest partition can already be near live while the restarted
    producer is still re-fetching the slower partitions' lost tails (D29); measuring from the
    newest tail declared recovery too early (2026-10-10: 11 offsets reported missing that
    arrived seconds later)."""
    c = Consumer(
        {"bootstrap.servers": S.bootstrap_servers, "group.id": "drill", "enable.auto.commit": False}
    )
    tails = []
    try:
        for p in range(S.main_topic.partitions):
            lo, hi = c.get_watermark_offsets(TopicPartition(S.main_topic.name, p), timeout=10)
            if hi <= lo:
                continue
            c.assign([TopicPartition(S.main_topic.name, p, hi - 1)])
            m = c.poll(10)
            raw = dict(m.headers() or []).get(H_CURSOR) if m and not m.error() else None
            tails.append(raw.decode() if raw else None)
    finally:
        c.close()
    cur = slowest(tails)
    eq = (
        [
            e["timestamp"]
            for e in json.loads(cur)
            if e["topic"].startswith("eqiad") and "timestamp" in e
        ]
        if cur
        else []
    )
    return (now_ms() - eq[0]) / 1000 if eq else None


def wait_until(desc: str, fn, timeout_s: float, every_s: float = 10) -> float:
    t0 = time.monotonic()
    while True:
        if fn():
            return time.monotonic() - t0
        if time.monotonic() - t0 > timeout_s:
            raise TimeoutError(f"gave up waiting for: {desc}")
        time.sleep(every_s)


def recovered() -> bool:
    try:
        highs, ends = kafka_highs(), landed_ends()
        lag = eqiad_lag_s()
    except KafkaException:  # the broker is still coming back (first kill drill: harness died)
        return False
    complete_landed = all(
        highs[p] - (0 if ends[p] is None else ends[p] + 1) < S.offsets_per_file for p in highs
    )
    return complete_landed and lag is not None and lag < 120


def wait_for_files_past(at: dict[int, int | None]) -> float:
    """Wait until every partition has landed a file beyond its landed end in `at`, so that the
    files straddling the failure exist and get deep-checked. Without this a drill can pass
    having re-derived nothing (the first mid_range and Kafka kill drills: 0 files)."""

    def past() -> bool:
        return all(e is not None and (at[p] is None or e > at[p]) for p, e in landed_ends().items())

    return wait_until("a new file landed in every partition", past, 3600, 20)


def verify_window(since_ms: int) -> dict:
    audit = reconcile(collect(S, since_ms))
    landing = verify_landing(S, boto3.client("s3"), since_ms)
    return {
        "audit": {
            k: audit[k]
            for k in ("verdict", "messages", "missing_upstream_offsets", "main_duplicate_event_ids")
        },
        "landing": {
            k: landing[k] for k in ("verdict", "deep_checked", "rederived_identical", "failures")
        },
        "passed": audit["verdict"] == "reconciled" and landing["verdict"] == "ok",
    }


def write_report(name: str, body: dict) -> Path:
    REPORTS.mkdir(parents=True, exist_ok=True)
    path = REPORTS / f"{iso(now_ms()).replace(':', '')}-{name}.json"
    path.write_text(json.dumps(body, indent=2, default=str))
    return path


# ------------------------------------------------------------------ drills


def drill_crash(point: str, n: int, backlog_s: float) -> dict:
    service = point.split(".")[0]
    t0 = now_ms()
    before = {"kafka_highs": kafka_highs(), "landed_ends": landed_ends()}
    compose("stop", "-t", "60", service)
    if backlog_s:
        time.sleep(backlog_s)  # let complete ranges / upstream backlog build up
    drill_name = f"wikistream-drill-{service}"
    sh("docker", "rm", "-f", drill_name, check=False)
    compose("run", "-d", "--name", drill_name, "-e", f"WIKISTREAM_CRASH_AT={point}:{n}", service)
    crashed_after = wait_until(
        f"{drill_name} to exit", lambda: container(drill_name)["status"] == "exited", 1800, 2
    )
    state = container(drill_name)
    logs = container_logs(drill_name)
    crash_line = next((ln for ln in logs.splitlines() if "CRASH POINT" in ln), None)
    log_tail = "\n".join(logs.splitlines()[-15:])
    at_crash = {"kafka_highs": kafka_highs(), "landed_ends": landed_ends()}
    sh("docker", "rm", "-f", drill_name, check=False)
    compose("up", "-d", service)
    recovered_after = wait_until("recovery", recovered, 3600, 15)
    recovered_after += wait_for_files_past(at_crash["landed_ends"])
    result = verify_window(t0)
    return {
        "drill": f"crash {point}:{n}",
        "started": iso(t0),
        "before": before,
        "crash": {
            "exit_code": state.get("exit_code"),
            "line": crash_line,
            "seconds_to_crash": round(crashed_after),
            "state_at_crash": at_crash,
        },
        "recovered_in_s": round(recovered_after),
        "after": {"kafka_highs": kafka_highs(), "landed_ends": landed_ends()},
        "verification": result,
        "passed": result["passed"] and state.get("exit_code") == 137 and crash_line is not None,
        "log_tail": log_tail[-1500:],
    }


def drill_kill(target: str) -> dict:
    name = f"wikistream-{target}"
    t0 = now_ms()
    before = {
        "kafka_highs": kafka_highs(),
        "landed_ends": landed_ends(),
        "container": container(name),
    }
    kill = hard_kill(name)
    recovered_after = wait_until("recovery", recovered, 3600, 15)
    recovered_after += wait_for_files_past(before["landed_ends"])
    highs_after = kafka_highs()
    result = verify_window(t0 - 60_000)
    no_truncation = all(highs_after[p] >= before["kafka_highs"][p] for p in highs_after)
    return {
        "drill": f"kill -9 {target}",
        "started": iso(t0),
        "before": before,
        "kill": kill,
        "recovered_in_s": round(recovered_after),
        "after": {
            "kafka_highs": highs_after,
            "container": container(name),
            "others": {c: container(f"wikistream-{c}") for c in SERVICES},
        },
        "kafka_not_truncated": no_truncation,
        "verification": result,
        "passed": result["passed"] and no_truncation and kill["exit_codes"] == [137],
    }


def drill_soak(kills: int, min_gap: float, max_gap: float, seed: int) -> dict:
    rng = random.Random(seed)
    t0 = now_ms()
    events, at_last_kill = [], landed_ends()
    for i in range(kills):
        time.sleep(rng.uniform(min_gap, max_gap))
        target = rng.choices(["producer", "consumer", "kafka"], weights=[2, 2, 1])[0]
        name = f"wikistream-{target}"
        if container(name)["status"] != "running":  # still restarting from the last kill
            events.append({"i": i, "at": iso(now_ms()), "target": target, "skipped": True})
            continue
        at, at_last_kill = iso(now_ms()), landed_ends()
        events.append({"i": i, "at": at, "target": target, **hard_kill(name)})
        print(f"kill {i + 1}/{kills}: {target} at {at} -> {events[-1]}", flush=True)
    wait_until(
        "all containers running",
        lambda: all(container(f"wikistream-{c}")["status"] == "running" for c in SERVICES),
        600,
        5,
    )
    recovered_after = wait_until("recovery", recovered, 3600, 15)
    recovered_after += wait_for_files_past(at_last_kill)
    result = verify_window(t0)
    return {
        "drill": f"soak: {kills} random SIGKILLs (seed {seed})",
        "started": iso(t0),
        "kills": events,
        "restart_counts": {c: container(f"wikistream-{c}").get("restarts") for c in SERVICES},
        "recovered_in_s_after_last_kill": round(recovered_after),
        "verification": result,
        "passed": result["passed"]
        and all(e.get("exit_codes") == [137] for e in events if not e.get("skipped")),
    }


def _last_json(text: str) -> dict | None:
    """The verifiers print one indented JSON report last; only its first line starts with {."""
    i = 0 if text.startswith("{") else text.rfind("\n{") + 1
    return json.loads(text[i:]) if i or text.startswith("{") else None


def _db(*args: str) -> dict:
    return json.loads(sh("databricks", *args, "-o", "json"))


def drill_pipeline_cancel() -> dict:
    def safe_minute() -> bool:  # stay clear of the :00/:30 scheduled runs
        return datetime.now(UTC).minute % 30 in range(4, 21)

    wait_until("a minute clear of the schedule", safe_minute, 1800, 20)
    t0 = now_ms()
    first = _db("pipelines", "start-update", PIPELINE_ID)["update_id"]
    wait_until(
        "update RUNNING",
        lambda: _db("pipelines", "get-update", PIPELINE_ID, first)["update"]["state"] == "RUNNING",
        600,
        2,
    )
    time.sleep(3)  # flows have started
    sh("databricks", "pipelines", "stop", PIPELINE_ID)
    final_first = _db("pipelines", "get-update", PIPELINE_ID, first)["update"]["state"]
    second = _db("pipelines", "start-update", PIPELINE_ID)["update_id"]
    wait_until(
        "second update finished",
        lambda: (
            _db("pipelines", "get-update", PIPELINE_ID, second)["update"]["state"]
            in ("COMPLETED", "FAILED", "CANCELED")
        ),
        1200,
        10,
    )
    final_second = _db("pipelines", "get-update", PIPELINE_ID, second)["update"]["state"]
    checks = {}
    for script, extra in [
        ("verify_bronze.py", ["--landed-before", iso(t0)]),
        ("verify_silver.py", []),
        ("verify_gold.py", []),
    ]:
        p = subprocess.run(
            ["uv", "run", "python", str(ROOT / "scripts" / script), *extra],
            capture_output=True,
            text=True,
        )
        checks[script] = {
            "exit_code": p.returncode,
            "report": _last_json(p.stdout),
            "stderr_tail": p.stderr[-600:] if p.returncode else "",
        }
    return {
        "drill": "cancel a pipeline update mid-run, then run another",
        "started": iso(t0),
        "cancelled_update": {"id": first, "final_state": final_first},
        "next_update": {"id": second, "final_state": final_second},
        "checks": checks,
        "passed": final_second == "COMPLETED" and all(c["exit_code"] == 0 for c in checks.values()),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("crash")
    c.add_argument("--point", required=True)
    c.add_argument("--n", type=int, default=1)
    c.add_argument("--backlog-s", type=float, default=0)
    k = sub.add_parser("kill")
    k.add_argument("--target", choices=["producer", "consumer", "kafka"], required=True)
    so = sub.add_parser("soak")
    so.add_argument("--kills", type=int, default=20)
    so.add_argument("--min-gap", type=float, default=30)
    so.add_argument("--max-gap", type=float, default=150)
    so.add_argument("--seed", type=int, default=20261010)
    sub.add_parser("pipeline-cancel")
    a = ap.parse_args()

    if a.cmd == "crash":
        body, name = drill_crash(a.point, a.n, a.backlog_s), f"crash-{a.point}"
    elif a.cmd == "kill":
        body, name = drill_kill(a.target), f"kill-{a.target}"
    elif a.cmd == "soak":
        body, name = drill_soak(a.kills, a.min_gap, a.max_gap, a.seed), "soak"
    else:
        body, name = drill_pipeline_cancel(), "pipeline-cancel"
    path = write_report(name, body)
    summary = {k: body[k] for k in body if k not in ("log_tail", "before", "after", "kills")}
    print(json.dumps(summary, indent=2, default=str))
    print(f"report: {path.relative_to(ROOT)}")
    raise SystemExit(0 if body["passed"] else 1)


if __name__ == "__main__":
    main()
