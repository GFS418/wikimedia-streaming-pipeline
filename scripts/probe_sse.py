# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = ["httpx>=0.27"]
# ///
"""Read-only probe of Wikimedia EventStreams recentchange.

Records, per event: local receive time, the SSE id (Kafka topic/partition/offset
on Wikimedia's side), meta.dt, the edit `timestamp`, wiki, bot, type, byte size.
Writes JSONL for offline analysis; no parsing decisions are made here beyond
"is it valid JSON".
"""

import json
import sys
import time
from datetime import UTC, datetime

import httpx

URL = "https://stream.wikimedia.org/v2/stream/recentchange"
UA = "wiki-streaming-probe/0.1 (https://github.com/GFS418; read-only rate probe)"


def main(duration_s: int, out_path: str) -> None:
    deadline = time.monotonic() + duration_s
    last_event_id = None
    reconnects = 0
    stats = {"events": 0, "bad_json": 0, "canary": 0}
    with open(out_path, "w") as out:
        while time.monotonic() < deadline:
            headers = {"User-Agent": UA, "Accept": "text/event-stream"}
            if last_event_id:
                headers["Last-Event-ID"] = last_event_id
            try:
                with httpx.stream(
                    "GET", URL, headers=headers, timeout=httpx.Timeout(10, read=60)
                ) as r:
                    r.raise_for_status()
                    ev_id, data = None, []
                    for line in r.iter_lines():
                        if time.monotonic() >= deadline:
                            break
                        if line.startswith("id:"):
                            ev_id = line[3:].strip()
                        elif line.startswith("data:"):
                            data.append(line[5:].lstrip())
                        elif line == "" and data:
                            recv = datetime.now(UTC).isoformat()
                            raw = "\n".join(data)
                            rec = {"recv": recv, "sse_id": ev_id, "bytes": len(raw.encode())}
                            try:
                                e = json.loads(raw)
                                meta = e.get("meta", {})
                                rec |= {
                                    "meta_id": meta.get("id"),
                                    "meta_dt": meta.get("dt"),
                                    "domain": meta.get("domain"),
                                    "partition": meta.get("partition"),
                                    "offset": meta.get("offset"),
                                    "ts": e.get("timestamp"),
                                    "wiki": e.get("wiki"),
                                    "bot": e.get("bot"),
                                    "type": e.get("type"),
                                    "keys": sorted(e.keys()),
                                }
                                if meta.get("domain") == "canary":
                                    stats["canary"] += 1
                            except json.JSONDecodeError:
                                stats["bad_json"] += 1
                                rec["bad_json"] = True
                            out.write(json.dumps(rec) + "\n")
                            stats["events"] += 1
                            last_event_id = ev_id or last_event_id
                            ev_id, data = None, []
            except (httpx.HTTPError, httpx.StreamError) as exc:
                reconnects += 1
                print(f"reconnect #{reconnects}: {type(exc).__name__}: {exc}", file=sys.stderr)
                time.sleep(min(2**reconnects, 30))
    print(json.dumps(stats | {"reconnects": reconnects}))


if __name__ == "__main__":
    main(int(sys.argv[1]), sys.argv[2])
