"""A local stand-in for Wikimedia EventStreams with its observed resume semantics (D31).

- Each event's SSE id lists every upstream partition: for those that have sent an event on this
  connection, the latest such event's timestamp + `id_offset_ms` (live: always + 1 ms, 2,620 of
  2,620 events on 2026-10-10, out-of-order ones included); for the rest {"offset": -1}
  (observed after reconnects, D22).
- A resume (`Last-Event-ID`, or `?since=` for all partitions) starts each partition at its
  EARLIEST event stamped at or after the requested time (Kafka offsetsForTimes); an
  {"offset": -1} entry, or a partition not mentioned, starts at the end ("now").
- No resume at all starts from the beginning (the real service starts from now).
- `cut_at`: (topic, offset) pairs; the first connection to send one closes right after it
  (each cut fires once). At the end of the log a connection idles 1 s, then closes.
"""

from __future__ import annotations

import json
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


def iso_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def make_event(base: dict, topic: str, offset: int, ts_ms: int) -> dict:
    """A contract-valid recentchange event at an upstream position and timestamp."""
    meta = base["meta"] | {
        "id": f"fake-{topic.split('.')[0]}-{offset}",
        "topic": topic,
        "partition": 0,
        "offset": offset,
        "dt": iso_ms(ts_ms),
    }
    data = json.dumps(base | {"meta": meta})
    return {"topic": topic, "partition": 0, "offset": offset, "ts": ts_ms, "data": data}


class FakeEventStreams:
    def __init__(self, events: list[dict], cut_at=(), id_offset_ms: int = 1):
        self.events = events  # in delivery order; each partition's events in offset order
        self.cut_at = set(cut_at)
        self.id_offset_ms = id_offset_ms
        self.partitions = sorted({(e["topic"], e["partition"]) for e in events})
        self.resumes: list[str | None] = []  # Last-Event-ID (or since=) of each connection
        self._stop = threading.Event()
        self._lock = threading.Lock()
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 (http.server API)
                fake._serve(self)

            def log_message(self, *args):
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_port}/v2/stream/recentchange"

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._server.shutdown()
        self._server.server_close()

    def _first_at_or_after(self, key, ts: int) -> int:
        offs = [
            e["offset"]
            for e in self.events
            if (e["topic"], e["partition"]) == key and e["ts"] >= ts
        ]
        return min(offs) if offs else self._end(key)

    def _end(self, key) -> int:
        offs = [e["offset"] for e in self.events if (e["topic"], e["partition"]) == key]
        return max(offs) + 1

    def _starts(self, last_id: str | None, since: str | None) -> dict:
        if since:
            ts = int(datetime.fromisoformat(since.replace("Z", "+00:00")).timestamp() * 1000)
            return {k: self._first_at_or_after(k, ts) for k in self.partitions}
        if not last_id:
            return {k: self._first_at_or_after(k, -1) for k in self.partitions}
        starts = {k: self._end(k) for k in self.partitions}
        for entry in json.loads(last_id):
            k = (entry["topic"], int(entry["partition"]))
            if k in starts and "timestamp" in entry:
                starts[k] = self._first_at_or_after(k, int(entry["timestamp"]))
        return starts

    def _serve(self, h: BaseHTTPRequestHandler) -> None:
        last_id = h.headers.get("Last-Event-ID")
        since = parse_qs(urlparse(h.path).query).get("since", [None])[0]
        with self._lock:
            self.resumes.append(last_id or (f"since={since}" if since else None))
        starts = self._starts(last_id, since)
        h.send_response(200)
        h.send_header("Content-Type", "text/event-stream")
        h.end_headers()
        latest = {}
        try:
            for e in self.events:
                k = (e["topic"], e["partition"])
                if e["offset"] < starts[k]:
                    continue
                latest[k] = e["ts"]
                ids = [
                    {"topic": t, "partition": p, "timestamp": latest[(t, p)] + self.id_offset_ms}
                    if (t, p) in latest
                    else {"topic": t, "partition": p, "offset": -1}
                    for t, p in self.partitions
                ]
                h.wfile.write(
                    f"event: message\nid: {json.dumps(ids)}\ndata: {e['data']}\n\n".encode()
                )
                h.wfile.flush()
                with self._lock:
                    cut = (e["topic"], e["offset"]) in self.cut_at
                    self.cut_at.discard((e["topic"], e["offset"]))
                if cut:
                    return  # the server closes the connection
            self._stop.wait(1)  # end of the log: idle briefly, then close (the client reconnects)
        except (BrokenPipeError, ConnectionResetError):
            return
