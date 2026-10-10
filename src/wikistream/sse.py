"""Server-sent events: a pure parser plus a reconnecting client for Wikimedia EventStreams.

The parser follows the WHATWG SSE rules that matter here: multi-line `data:`, the
last-event-ID buffer persisting across events, comments, and a single optional space
after the colon. The client resumes with `Last-Event-ID` after every disconnect
(Wikimedia closes connections every 15 minutes; our probe also saw an unscheduled one),
sending `resume_from(last_event_id)` so the caller can resume earlier than the id (D31),
and can be asked to drop the connection and re-read (`reread`).
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass

import httpx

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SSEEvent:
    data: str
    id: str | None
    event: str = "message"


class SSEParser:
    def __init__(self) -> None:
        self.last_event_id: str | None = None
        self._data: list[str] = []
        self._event = ""

    def feed(self, line: str) -> SSEEvent | None:
        """Feed one line (without its terminator). Returns an event on dispatch."""
        if line == "":
            return self._dispatch()
        if line.startswith(":"):
            return None
        name, sep, value = line.partition(":")
        if sep and value.startswith(" "):
            value = value[1:]
        if name == "data":
            self._data.append(value)
        elif name == "id":
            if "\0" not in value:
                self.last_event_id = value
        elif name == "event":
            self._event = value
        # "retry" and unknown fields are ignored; the client owns its backoff policy.
        return None

    def _dispatch(self) -> SSEEvent | None:
        data, event = self._data, self._event or "message"
        self._data, self._event = [], ""
        if not data:
            return None
        return SSEEvent(data="\n".join(data), id=self.last_event_id, event=event)

    def parse(self, lines: Iterable[str]) -> Iterator[SSEEvent]:
        for line in lines:
            ev = self.feed(line)
            if ev is not None:
                yield ev


class SSEClient:
    """Yields events forever (until `stop` is set), reconnecting with backoff."""

    def __init__(
        self,
        url: str,
        user_agent: str,
        last_event_id: str | None = None,
        stop: threading.Event | None = None,
        max_backoff_s: float = 30.0,
        resume_from: Callable[[str], str | None] | None = None,
    ) -> None:
        self.url = url
        self.user_agent = user_agent
        self.last_event_id = last_event_id
        self.stop = stop or threading.Event()
        self.max_backoff_s = max_backoff_s
        self.resume_from = resume_from or (lambda cursor: cursor)
        self.reconnects = 0
        self.connections = 0  # successful connects; a change tells the caller a resume began
        self._reread = False

    def reread(self) -> None:
        """Drop the current connection and reconnect at once from `last_event_id` (D31)."""
        self._reread = True

    def events(self) -> Iterator[SSEEvent]:
        failures = 0
        while not self.stop.is_set():
            headers = {"User-Agent": self.user_agent, "Accept": "text/event-stream"}
            if self.last_event_id:
                headers["Last-Event-ID"] = self.resume_from(self.last_event_id)
            parser = SSEParser()
            parser.last_event_id = self.last_event_id
            try:
                timeout = httpx.Timeout(10.0, read=60.0)
                with httpx.stream("GET", self.url, headers=headers, timeout=timeout) as resp:
                    resp.raise_for_status()
                    self.connections += 1
                    self._reread = False  # a new connection already resumes from the cursor
                    for ev in parser.parse(resp.iter_lines()):
                        failures = 0
                        self.last_event_id = ev.id or self.last_event_id
                        yield ev
                        if self.stop.is_set():
                            return
                        if self._reread:
                            break
            except (httpx.HTTPError, httpx.StreamError) as exc:
                failures += 1
                self.reconnects += 1
                delay = min(2 ** (failures - 1), self.max_backoff_s)
                log.warning(
                    "sse disconnect #%d (%s: %s); retry in %.0fs",
                    self.reconnects,
                    type(exc).__name__,
                    exc,
                    delay,
                )
                self.stop.wait(delay)
                continue
            if self._reread:
                self._reread = False
                self.reconnects += 1
                log.info("sse re-read requested; reconnect #%d", self.reconnects)
                continue
            # Server closed the stream cleanly; reconnect immediately with the cursor.
            self.reconnects += 1
            log.info("sse stream ended cleanly; reconnect #%d", self.reconnects)
            time.sleep(0.5)
