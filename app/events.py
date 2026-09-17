"""Event-timeline + push-channel layer.

`prod.eu-central-1.event-timeline-api.tractive.com/api/1/events` returns
the same walks / safezoneEnter|Exit / dangerzoneEnter|Exit /
dailyActivityGoalReached / insidePowerSavingZone feed the Tractive iOS
app shows in its activity timeline. We poll it on the MED tier and
incrementally publish each NEW event (by `id`) as:

  - one-shot `tractive/<pet_id>/events/<type>` JSON for InfluxDB
  - retained `tractive/<pet_id>/last_<type>_*` state mirrors for HA

`channel.tractive.com/3/channel` is a server-streaming long-poll that
emits position/hw_info/tracker_status updates in real time. A daemon
thread holds the connection and dispatches each event into the existing
publish helpers via a caller-supplied callback. The main poll loop
keeps the FAST tier running as a backstop if the channel is silent.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import threading
import time
from typing import Callable

import requests
from ha_mqtt_bridge import request_with_backoff

from parsers import TimelineEvent, parse_events


EVENT_TL_BASE = "https://prod.eu-central-1.event-timeline-api.tractive.com/api/1/"
CHANNEL_URL = "https://channel.tractive.com/3/channel"

# Cap the in-memory seen-events set so it can't grow unbounded over a
# long-running bridge. 500 events ≈ ~1 month of typical home activity
# for a single pet.
SEEN_EVENTS_CAP = 500

# Channel silence beyond this many seconds → treat as disconnected and
# let the FAST poll backstop drive updates until a reconnect succeeds.
CHANNEL_STALE_AFTER = 120

# Server keep-alive cadence on the channel is ~30s. Force a reconnect if
# we go this long with no bytes at all.
CHANNEL_KEEPALIVE_TIMEOUT = 90


# ============================================================== Event timeline


class EventStream:
    """Stateful incremental poller for the event-timeline feed."""

    def __init__(self, pet_id: str, timezone_name: str = "UTC"):
        self.pet_id = pet_id
        self.timezone_name = timezone_name
        self.seen_ids: set[str] = set()
        self._seen_order: list[str] = []  # FIFO for trimming
        # If True, the next poll suppresses publishing (used to populate
        # seen_ids on first run without spamming HA — initial-fill of
        # InfluxDB is handled by `backfill()`).
        self._suppress_publish_once = False

    def _remember(self, event_id: str) -> None:
        if event_id in self.seen_ids:
            return
        self.seen_ids.add(event_id)
        self._seen_order.append(event_id)
        if len(self._seen_order) > SEEN_EVENTS_CAP:
            drop = self._seen_order[: len(self._seen_order) - SEEN_EVENTS_CAP]
            for d in drop:
                self.seen_ids.discard(d)
            self._seen_order = self._seen_order[-SEEN_EVENTS_CAP:]

    def fetch_page(self, token: str, auth_headers_fn, params: dict) -> dict:
        """GET with exponential backoff on 429/5xx (shared toolkit
        helper — same one `main.py`'s `_request` uses). Previously this
        raised a bare, unretried `RuntimeError` on the first 429; the
        MED-cycle caller's `except Exception` swallowed it (non-fatal),
        but every rate-limited poll was silently dropped instead of
        retried."""
        headers = auth_headers_fn(token)
        r = request_with_backoff(
            "GET", EVENT_TL_BASE + "events", headers=headers, params=params, timeout=20,
        )
        if r.status_code == 401:
            raise PermissionError("event-timeline 401")
        r.raise_for_status()
        return r.json()

    def fetch_window(self, token: str, auth_headers_fn, types: list[str],
                     since_date: str | None = None, max_pages: int = 4) -> list[TimelineEvent]:
        """Walk pagination backward up to `max_pages` (or until we hit
        events older than `since_date`). Returns newest-first."""
        params: dict = {
            "petId": self.pet_id,
            "timezone": self.timezone_name,
            "types": ",".join(types),
        }
        if since_date:
            params["resumeFrom"] = since_date

        all_events: list[TimelineEvent] = []
        next_url: str | None = None
        for _ in range(max_pages):
            if next_url:
                # next_url already carries petId/timezone/types/resumeFrom.
                # Re-parse just the query string into params.
                from urllib.parse import urlparse, parse_qs
                q = parse_qs(urlparse(next_url).query)
                params = {k: v[0] if isinstance(v, list) and len(v) == 1 else v for k, v in q.items()}
            page = self.fetch_page(token, auth_headers_fn, params)
            evts = parse_events(page)
            if not evts:
                break
            all_events.extend(evts)
            next_url = (page.get("paging") or {}).get("nextUrl")
            if not next_url:
                break
        return all_events

    def backfill(self, token: str, auth_headers_fn, types: list[str],
                 days: int, on_event: Callable[[TimelineEvent], None],
                 log: logging.Logger) -> int:
        """One-shot at startup. Publishes (for InfluxDB) every event
        from the past `days` days, and seeds `seen_ids` so the next
        incremental poll doesn't re-publish anything."""
        if days <= 0:
            return 0
        since = (dt.date.today() - dt.timedelta(days=days)).isoformat()
        try:
            events = self.fetch_window(token, auth_headers_fn, types, since_date=since, max_pages=8)
        except Exception:
            log.exception("event-timeline backfill failed (non-fatal)")
            return 0
        # Sort oldest-first so on_event sees chronological order.
        events.sort(key=lambda e: e.event_ts_iso or "")
        for evt in events:
            if not evt.id or evt.id in self.seen_ids:
                continue
            on_event(evt)
            self._remember(evt.id)
        log.info("event-timeline backfill: %d events over last %dd", len(events), days)
        return len(events)

    def poll_incremental(self, token: str, auth_headers_fn, types: list[str],
                         on_event: Callable[[TimelineEvent], None],
                         log: logging.Logger) -> int:
        """Fetch the first (most-recent) page; publish any IDs we haven't
        seen yet. Bounded to one page since we expect <20 new events per
        MED-cycle window."""
        try:
            events = self.fetch_window(token, auth_headers_fn, types, max_pages=1)
        except Exception:
            log.exception("event-timeline poll failed (non-fatal)")
            return 0
        new = [e for e in events if e.id and e.id not in self.seen_ids]
        # Publish chronologically.
        new.sort(key=lambda e: e.event_ts_iso or "")
        for evt in new:
            on_event(evt)
            self._remember(evt.id)
        if new:
            log.info("event-timeline: %d new events (types=%s)",
                     len(new), ",".join(sorted({e.type for e in new})))
        return len(new)


# ============================================================== Push channel


class ChannelClient:
    """Daemon-thread long-poll consumer of channel.tractive.com/3/channel.

    The channel emits newline-delimited JSON messages. Known shapes:

      {"type": "keep-alive"}
      {"type": "handshake", ...}
      {"message": "tracker_status", "tracker_id": "...", ...}
      {"message": "position", "tracker_id": "...", ...}
      {"message": "hw_info", "tracker_id": "...", ...}

    `on_message` is invoked from the channel thread with the parsed dict
    for any line whose `type` is not in the IGNORE set. Callers should
    keep callbacks cheap — work that triggers MQTT publishes is fine,
    but blocking on REST calls would stall the channel.
    """

    IGNORE_TYPES = {"keep-alive", "handshake"}

    def __init__(self, auth_headers_fn: Callable[[], dict],
                 on_message: Callable[[dict], None],
                 log: logging.Logger):
        self._auth_headers_fn = auth_headers_fn
        self._on_message = on_message
        self._log = log
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._last_message_at: float = 0.0
        self._connected: bool = False

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def stale(self) -> bool:
        """True if the channel is connected but nothing (not even a
        keep-alive) has arrived in CHANNEL_STALE_AFTER seconds.

        A channel that is not connected is not stale: it is starting, or
        its own loop is already reconnecting with backoff. Replacing it then
        resets that backoff and briefly runs two connections, which is how
        the first version of this check drew a 429 on startup (2026-09-17).
        """
        if not self._connected:
            return False
        return (time.time() - self._last_message_at) > CHANNEL_STALE_AFTER

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="tractive-channel", daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                self._listen_one_session()
                backoff = 1.0  # successful disconnect — fast reconnect
            except PermissionError as e:
                # Token expired mid-stream; let the main loop re-auth on
                # its next iteration. Don't hot-loop.
                self._log.warning("channel 401: %s — sleeping 30s for re-auth", e)
                self._connected = False
                self._stop.wait(30)
            except Exception as e:
                self._log.warning("channel error: %s — backoff %.0fs", e, backoff)
                self._connected = False
                self._stop.wait(backoff)
                backoff = min(backoff * 2, 60)

    def _listen_one_session(self) -> None:
        headers = self._auth_headers_fn()
        # `requests` will issue a POST that the server holds open for
        # streaming. `iter_lines` yields each newline-terminated frame.
        with requests.post(
            CHANNEL_URL, headers=headers,
            stream=True, timeout=(10, CHANNEL_KEEPALIVE_TIMEOUT + 30),
        ) as r:
            if r.status_code in (401, 403):
                raise PermissionError(f"channel HTTP {r.status_code}")
            if r.status_code == 429:
                raise RuntimeError("channel 429")
            r.raise_for_status()
            self._connected = True
            self._last_message_at = time.time()
            self._log.info("channel connected")
            for raw_line in r.iter_lines(decode_unicode=True, chunk_size=512):
                if self._stop.is_set():
                    break
                if not raw_line:
                    continue
                # Tractive frames are JSON dicts. Anything else we skip.
                try:
                    msg = json.loads(raw_line)
                except (ValueError, TypeError):
                    continue
                self._last_message_at = time.time()
                mtype = msg.get("type")
                if mtype in self.IGNORE_TYPES:
                    continue
                try:
                    self._on_message(msg)
                except Exception:
                    self._log.exception("channel on_message handler raised")
        self._connected = False
        self._log.info("channel disconnected")
