"""
Tractive Cloud → MQTT bridge.

Polls the Tractive graph API + APS API on a tiered cadence and publishes
~50 HA-discovery entities, including everything the official `tractive`
HA integration leaves unmapped:

  - biometric statuses + numeric vitals (bark/RHR/RR/scratch + RHR in bpm
    + RR in br/min)
  - daily activity numerics (active min, goal, %, calories, streak, breed
    deviation, age group)
  - weekly health report (avg activity, calories, sleep, sleep interrupts,
    RHR, RR, prose summary)
  - health-monitor statuses (6 monitor types — activity/RHR/RR/sleep/
    separation/long-term activity)
  - health alerts history (total count, last alert metadata)
  - separation phases (ongoing detection)
  - tracker state metadata (Power Saving vs Operational, battery state,
    charging state, firmware, hw edition, capabilities)
  - position metadata (uncertainty, sensor type, accuracy bucket)
  - account-scoped data (email, country, unit preferences, notification
    count, share count, subscription status)

Co-exists with HA's official `tractive` integration without collision —
all entities live under separate HA devices `Tractive Bridge: <pet>` and
`Tractive Account`, with `unique_id` prefixes `tractive_mqtt_bridge_<pet_id>_*`
and `tractive_mqtt_bridge_account_*`.

API surface live-probed 2026-05-22 (Phase 1) + 2026-05-26 (Phase 2).

Endpoints by tier:

  FAST  (90 s) — every cycle:
    GET /4/tracker/{dev_id}                 (tracker state)
    GET /4/device_hw_report/{dev_id}        (battery_state)
    GET /4/device_pos_report/{dev_id}       (pos_uncertainty, sensor_used)
    GET /api/1/pet/{pet_id}/health/overview (biometric statuses, alerts count)

  MED   (300 s):
    GET /api/2/pet/{pet_id}/activity/day-overview?date=
    GET /api/1/pet/{pet_id}/bark/day-overview?date=
    GET /api/1/pet/{pet_id}/scratch/day-overview?date=
    GET /api/1/pet/{pet_id}/health-alerts/status
    GET /api/1/pet/{pet_id}/health-alerts/history
    GET /api/1/pet/{pet_id}/separation/phases?states=ONGOING
    GET /4/user/{uid}/notifications

  SLOW  (3600 s):
    GET /api/1/pet/{pet_id}/activity/week-overview?date=
    GET /api/1/pet/{pet_id}/health/weekly-report/summary
    GET /api/1/pet/{pet_id}/health/weekly-report/data
    GET /4/user/{uid}                       (account-scoped state)
    GET /4/user/{uid}/shares
    GET /4/user/{uid}/subscriptions
    GET /4/user/{uid}/subscriptions/usage
    GET /4/trackable_object/{pet_id}        (pet details refresh)

  STARTUP — pet/tracker discovery + first publish of every tier.

Rate-limit aware: INTER_CALL_DELAY between every endpoint inside a cycle
(0.4 s default). Worst-case all-tiers-firing cycle is ~17 calls, but tiers
are interleaved so per-minute load stays around 5-6 req/min.

Module layout:
  - parsers.py    — dataclasses, parse_* helpers, _humanize_enum, _pos_accuracy_quality
  - discovery.py  — HA Discovery payload factory
  - main.py       — env config, HTTP client, publish helpers, multi-cadence scheduler
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import signal
import sys
import time
import uuid

import requests
from ha_mqtt_bridge import (
    ThreadedPublisher,
    configure_logging,
    register_github_error_reporter,
    request_with_backoff,
    watch_ha_birth,
)

from discovery import (
    HEALTH_MONITOR_TYPES,
    PERIOD_EVENT_TYPES,
    TIMELINE_EVENT_TYPES,
    ZONE_EVENT_TYPES,
    discovery_specs_account,
    discovery_specs_pet,
)
from events import ChannelClient, EventStream
from parsers import (
    HEALTH_MONITOR_HEALTHY,
    HEALTHY_STATUSES,
    SCRATCH_HEALTHY,
    ActivityDayOverview,
    ActivityWeekOverview,
    BarkDayOverview,
    HealthAlertsHistory,
    HealthAlertsStatus,
    HealthOverview,
    HwReport,
    Pet,
    PosReport,
    PositionRecord,
    ScratchDayOverview,
    SeparationPhases,
    Subscriptions,
    TimelineEvent,
    TrackerState,
    UserAccount,
    UserNotifications,
    UserShares,
    WeeklyReportData,
    WeeklyReportSummary,
    _humanize_enum,
    _pos_accuracy_quality,
    parse_activity_day_overview,
    parse_activity_week_overview,
    parse_bark_day_overview,
    parse_health_alerts_history,
    parse_health_alerts_status,
    parse_health_overview,
    parse_hw_report,
    parse_pos_report,
    parse_positions,
    parse_scratch_day_overview,
    parse_separation_phases,
    parse_subscriptions,
    parse_tracker,
    parse_user_account,
    parse_user_notifications,
    parse_user_shares,
    parse_weekly_report_data,
    parse_weekly_report_summary,
)


# --- production-error reporter ---------------------------------------
# Installs sys.excepthook + threading.excepthook so every uncaught
# exception flows through GitHub repository_dispatch → the Production
# Error Intake workflow → Claude auto-fix PR. Silently disabled when
# GITHUB_ERROR_TOKEN is unset (e.g. local dev).
register_github_error_reporter("tractive-mqtt-bridge")
# ---------------------------------------------------------------------
GRAPH_BASE = "https://graph.tractive.com/4/"
APS_BASE = "https://aps-api.tractive.com/api/1/"
APS_V2_BASE = "https://aps-api.tractive.com/api/2/"

USERNAME = os.environ["TRACTIVE_USERNAME"]
PASSWORD = os.environ["TRACTIVE_PASSWORD"]
USER_ID = os.environ["TRACTIVE_USER_ID"]
CLIENT_ID = os.environ["TRACTIVE_CLIENT_ID"]

MQTT_HOST = os.environ.get("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "")
MQTT_PASS = os.environ["MQTT_PASSWORD"]
# Off by default (current behaviour) — set MQTT_TLS=1 for a broker that
# requires TLS; MQTT_CA_FILE points at a custom CA bundle (system trust
# store is used when unset).
MQTT_TLS = os.environ.get("MQTT_TLS", "0") != "0"
MQTT_CA_FILE = os.environ.get("MQTT_CA_FILE") or None

# `POLL_INTERVAL` is the Phase-1 legacy name for FAST_POLL_INTERVAL — kept
# as a fallback for the existing .env file (default 90 s).
FAST_POLL = int(os.environ.get("FAST_POLL_INTERVAL", os.environ.get("POLL_INTERVAL", "90")))
MED_POLL = int(os.environ.get("MED_POLL_INTERVAL", "300"))
SLOW_POLL = int(os.environ.get("SLOW_POLL_INTERVAL", "3600"))
INTER_CALL_DELAY = float(os.environ.get("INTER_CALL_DELAY", "0.4"))

# Hours of position history to backfill into MQTT (and onward into
# InfluxDB) on bridge startup. Capped at 24 because Tractive rejects
# /positions queries spanning > 24h with HTTP 400; multi-day backfill
# would need pagination. 0 disables backfill entirely.
POSITION_BACKFILL_HOURS = int(os.environ.get("POSITION_BACKFILL_HOURS", "24"))

# Days of timeline-event history (walks, geofence crossings, daily-goal
# achievements) to backfill into InfluxDB on startup. 0 disables.
EVENT_BACKFILL_DAYS = int(os.environ.get("EVENT_BACKFILL_DAYS", "30"))

# Local IANA timezone the event-timeline API treats as the user's wall
# clock. Tractive's events API requires a tz so it can compute the
# "today started at 00:00" boundaries used by dailyActivityGoalReached.
EVENT_TIMEZONE = os.environ.get("EVENT_TIMEZONE", "UTC")

# Push-channel toggle. When enabled (default) the bridge maintains a
# long-poll connection to channel.tractive.com:443/3/channel; tracker
# state / position / hw_info updates arrive within ~2s of an event
# happening on the tracker. The FAST poll continues to run as a
# backstop for when the channel is silent or disconnected.
CHANNEL_ENABLED = os.environ.get("CHANNEL_ENABLED", "1") != "0"

DISCOVERY_PREFIX = os.environ.get("HA_DISCOVERY_PREFIX", "homeassistant")
TOPIC_PREFIX = os.environ.get("MQTT_TOPIC_PREFIX", "tractive")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

BRIDGE_LWT_TOPIC = f"{TOPIC_PREFIX}/bridge/online"


# -------------------------------------------------------------- API client


def _request(method: str, url: str, *, headers: dict | None = None, json_body: dict | None = None, params: dict | None = None):
    """Exponential backoff on 429/5xx (shared toolkit helper — also used
    by `events.py`'s `EventStream.fetch_page`). A 429/5xx that survives
    every retry raises `RetryExhaustedError` (a `RuntimeError`
    subclass); the main loop's per-cycle `except Exception` guards catch
    that instead of it killing the process — previously this raised a
    bare, unguarded `RuntimeError` straight out of pet/tracker discovery."""
    r = request_with_backoff(method, url, headers=headers, json_body=json_body, params=params, timeout=15)
    if r.status_code == 401:
        raise PermissionError(f"401 from {url}")
    r.raise_for_status()
    # `/4/user/{uid}/notifications` returns the literal empty string when
    # there are no notifications, which would explode r.json(). Treat any
    # empty body as None and let the parsers normalise.
    if not r.content:
        return None
    try:
        return r.json()
    except ValueError:
        return r.text



def _request_optional(method: str, url: str, **kw):
    """Like `_request`, but treats an empty 400 as "this resource has no data".

    Tractive answers 400 with an EMPTY BODY -- not 404, not 200-with-no-body --
    for a health report a pet has no data for: no health subscription, a
    tracker that has not accumulated a week of history, a species the vitality
    metrics do not cover. That is a steady state, not a failure, but
    `raise_for_status` turned it into an exception that aborted the pet's whole
    slow cycle before `publish_weekly_report`, so 379 consecutive cycles
    published nothing and logged a traceback each time.

    A 400 that carries a body is still a real error and still raises.
    """
    try:
        return _request(method, url, **kw)
    except requests.HTTPError as e:
        r = e.response
        if r is not None and r.status_code == 400 and not (r.content or b"").strip():
            return None
        raise


def _base_headers() -> dict:
    return {
        "x-tractive-client": CLIENT_ID,
        "content-type": "application/json;charset=UTF-8",
        "accept": "application/json, text/plain, */*",
    }


def auth_token() -> tuple[str, int]:
    """Return (access_token, expires_at_epoch). Tokens last 60 days."""
    data = _request("POST", f"{GRAPH_BASE}auth/token", headers=_base_headers(), json_body={
        "platform_email": USERNAME,
        "platform_token": PASSWORD,
        "grant_type": "tractive",
    })
    return data["access_token"], int(data["expires_at"])


def _auth_headers(token: str) -> dict:
    return {**_base_headers(), "x-tractive-user": USER_ID, "authorization": f"Bearer {token}"}


# --- Phase 1 endpoints ---


def fetch_trackable_objects(token: str) -> list[dict]:
    return _request("GET", f"{GRAPH_BASE}user/{USER_ID}/trackable_objects", headers=_auth_headers(token))


def fetch_pet(pet_id: str, token: str) -> dict:
    return _request("GET", f"{GRAPH_BASE}trackable_object/{pet_id}", headers=_auth_headers(token))


def fetch_tracker(dev_id: str, token: str) -> dict:
    return _request("GET", f"{GRAPH_BASE}tracker/{dev_id}", headers=_auth_headers(token))


def fetch_hw_report(dev_id: str, token: str) -> dict:
    return _request("GET", f"{GRAPH_BASE}device_hw_report/{dev_id}", headers=_auth_headers(token))


def fetch_pos_report(dev_id: str, token: str) -> dict:
    return _request("GET", f"{GRAPH_BASE}device_pos_report/{dev_id}", headers=_auth_headers(token))


def fetch_positions(dev_id: str, token: str, time_from: int, time_to: int) -> list:
    """Window must be <= 24 h; longer ranges return HTTP 400. Caller paginates."""
    return _request(
        "GET", f"{GRAPH_BASE}tracker/{dev_id}/positions",
        headers=_auth_headers(token),
        params={"time_from": time_from, "time_to": time_to, "format": "json_segments"},
    )


def fetch_health_overview(pet_id: str, token: str) -> dict:
    return _request("GET", f"{APS_BASE}pet/{pet_id}/health/overview", headers=_auth_headers(token))


# --- Phase 2 endpoints ---


def fetch_activity_day_overview(pet_id: str, token: str, date_iso: str) -> dict:
    return _request(
        "GET", f"{APS_V2_BASE}pet/{pet_id}/activity/day-overview",
        headers=_auth_headers(token), params={"date": date_iso},
    )


def fetch_activity_week_overview(pet_id: str, token: str, date_iso: str) -> dict:
    return _request(
        "GET", f"{APS_BASE}pet/{pet_id}/activity/week-overview",
        headers=_auth_headers(token), params={"date": date_iso},
    )


def fetch_weekly_report_summary(pet_id: str, token: str) -> dict:
    return _request_optional("GET", f"{APS_BASE}pet/{pet_id}/health/weekly-report/summary", headers=_auth_headers(token))


def fetch_weekly_report_data(pet_id: str, token: str) -> dict:
    return _request_optional("GET", f"{APS_BASE}pet/{pet_id}/health/weekly-report/data", headers=_auth_headers(token))


def fetch_health_alerts_status(pet_id: str, token: str) -> dict:
    return _request("GET", f"{APS_BASE}pet/{pet_id}/health-alerts/status", headers=_auth_headers(token))


def fetch_health_alerts_history(pet_id: str, token: str) -> dict:
    return _request("GET", f"{APS_BASE}pet/{pet_id}/health-alerts/history", headers=_auth_headers(token))


def fetch_bark_day_overview(pet_id: str, token: str, date_iso: str) -> dict:
    return _request(
        "GET", f"{APS_BASE}pet/{pet_id}/bark/day-overview",
        headers=_auth_headers(token), params={"date": date_iso},
    )


def fetch_scratch_day_overview(pet_id: str, token: str, date_iso: str) -> dict:
    return _request(
        "GET", f"{APS_BASE}pet/{pet_id}/scratch/day-overview",
        headers=_auth_headers(token), params={"date": date_iso},
    )


def fetch_separation_phases_ongoing(pet_id: str, token: str) -> list:
    return _request(
        "GET", f"{APS_BASE}pet/{pet_id}/separation/phases",
        headers=_auth_headers(token), params={"states": "ONGOING"},
    )


def fetch_user(token: str) -> dict:
    return _request("GET", f"{GRAPH_BASE}user/{USER_ID}", headers=_auth_headers(token))


def fetch_user_notifications(token: str):
    return _request("GET", f"{GRAPH_BASE}user/{USER_ID}/notifications", headers=_auth_headers(token))


def fetch_user_shares(token: str) -> list:
    return _request("GET", f"{GRAPH_BASE}user/{USER_ID}/shares", headers=_auth_headers(token))


def fetch_user_subscriptions(token: str) -> list:
    return _request("GET", f"{GRAPH_BASE}user/{USER_ID}/subscriptions", headers=_auth_headers(token))


def fetch_user_subscriptions_usage(token: str) -> list:
    return _request("GET", f"{GRAPH_BASE}user/{USER_ID}/subscriptions/usage", headers=_auth_headers(token))


# -------------------------------------------------------------- publish helpers


def publish_health(pub: ThreadedPublisher, pet_id: str, h: HealthOverview) -> None:
    base = f"{TOPIC_PREFIX}/{pet_id}"
    def on_off(status: str | None, healthy: set[str]) -> str:
        if status is None: return "OFF"
        return "OFF" if status in healthy else "ON"

    if h.bark_status:
        pub.publish_state(f"{base}/bark_status", _humanize_enum(h.bark_status))
    if h.heart_rate_status:
        pub.publish_state(f"{base}/heart_rate_status", _humanize_enum(h.heart_rate_status))
    if h.respiratory_rate_status:
        pub.publish_state(f"{base}/respiratory_rate_status", _humanize_enum(h.respiratory_rate_status))
    if h.scratch_status:
        pub.publish_state(f"{base}/scratch_status", _humanize_enum(h.scratch_status))
    pub.publish_state(f"{base}/bark_alert", on_off(h.bark_status, HEALTHY_STATUSES))
    pub.publish_state(f"{base}/heart_rate_alert", on_off(h.heart_rate_status, HEALTHY_STATUSES))
    pub.publish_state(f"{base}/respiratory_rate_alert", on_off(h.respiratory_rate_status, HEALTHY_STATUSES))
    pub.publish_state(f"{base}/scratch_alert", on_off(h.scratch_status, SCRATCH_HEALTHY))
    pub.publish_state(f"{base}/unseen_health_alerts", str(h.unseen_health_alerts))
    if h.data_synced_at:
        pub.publish_state(f"{base}/health_data_synced_at", h.data_synced_at)


def publish_tracker(pub: ThreadedPublisher, pet_id: str, t: TrackerState) -> None:
    base = f"{TOPIC_PREFIX}/{pet_id}"
    if t.state:
        pub.publish_state(f"{base}/tracker_state", _humanize_enum(t.state))
    if t.state_reason:
        pub.publish_state(f"{base}/tracker_state_reason", _humanize_enum(t.state_reason))
    if t.battery_state:
        pub.publish_state(f"{base}/tracker_battery_state", _humanize_enum(t.battery_state))
    if t.charging_state:
        pub.publish_state(f"{base}/tracker_charging_state", _humanize_enum(t.charging_state))
    if t.fw_version:
        # Raw product version, not an enum — passed through unchanged.
        pub.publish_state(f"{base}/tracker_firmware", t.fw_version)
    if t.hw_edition:
        pub.publish_state(f"{base}/tracker_hw_edition", _humanize_enum(t.hw_edition))


def publish_hw(pub: ThreadedPublisher, pet_id: str, h: HwReport) -> None:
    base = f"{TOPIC_PREFIX}/{pet_id}"
    # NOTE: when the device_hw_report fields are null we skip publishing
    # entirely so HA's "Unknown" state surfaces — only publish when there's a
    # real enum value to humanize. (Previously published the literal string
    # "unknown" which the recorder then stored as a real value.)
    if h.clip_mounted_state:
        pub.publish_state(f"{base}/clip_mounted_state", _humanize_enum(h.clip_mounted_state))
    if h.temperature_state:
        pub.publish_state(f"{base}/temperature_state", _humanize_enum(h.temperature_state))


def publish_pos(pub: ThreadedPublisher, pet_id: str, p: PosReport) -> None:
    """Fast-tier publish: live retained state for HA + a one-shot event for
    InfluxDB. Updates pos_sensor_used, pos_uncertainty, pos_accuracy_quality
    (Phase 1), latitude/longitude/altitude/speed/position_fix_at (Phase 3),
    and the device_tracker state + attrs (Phase 3)."""
    base = f"{TOPIC_PREFIX}/{pet_id}"
    if p.sensor_used:
        pub.publish_state(f"{base}/pos_sensor_used", _humanize_enum(p.sensor_used))
    if p.pos_uncertainty is not None:
        pub.publish_state(f"{base}/pos_uncertainty", str(p.pos_uncertainty))
        quality = _pos_accuracy_quality(p.pos_uncertainty)
        if quality is not None:
            pub.publish_state(f"{base}/pos_accuracy_quality", quality)
    # Phase 3 — numeric position state + device_tracker
    lat, lon = (p.latlong or [None, None])[:2] if p.latlong else (None, None)
    fix_iso: str | None = None
    if p.time is not None:
        fix_iso = dt.datetime.fromtimestamp(p.time, tz=dt.timezone.utc).isoformat()
        pub.publish_state(f"{base}/position_fix_at", fix_iso)
    if lat is not None:
        pub.publish_state(f"{base}/latitude", str(lat))
    if lon is not None:
        pub.publish_state(f"{base}/longitude", str(lon))
    if p.altitude is not None:
        pub.publish_state(f"{base}/altitude_m", str(p.altitude))
    if p.speed is not None:
        pub.publish_state(f"{base}/speed_ms", str(p.speed))

    # device_tracker: state = home if inside the Tractive-cloud-configured
    # Wi-Fi power-saving zone (KNOWN_WIFI), else not_home. Attrs carry the
    # full GPS-payload shape HA's map panel expects.
    dt_state = "home" if (p.sensor_used or "").upper() == "KNOWN_WIFI" else "not_home"
    pub.publish_state(f"{base}/device_tracker/state", dt_state)
    if lat is not None and lon is not None:
        attrs = {
            "latitude": lat,
            "longitude": lon,
            "gps_accuracy": p.pos_uncertainty,
            "altitude": p.altitude,
            "speed": p.speed,
            "source_type": "gps",
            "sensor_used": p.sensor_used,
        }
        if fix_iso:
            attrs["fix_at"] = fix_iso
        pub.publish_raw(f"{base}/device_tracker/attrs", json.dumps(attrs), retain=True)

    # One-shot event for InfluxDB ingest — published unretained so it
    # doesn't accumulate, with embedded `ts` so telegraf back-dates the
    # InfluxDB point to the position's actual fix time.
    if lat is not None and lon is not None and p.time is not None:
        _publish_position_event(pub, pet_id, PositionRecord(
            time_epoch=p.time, latitude=lat, longitude=lon,
            altitude=p.altitude, speed=p.speed,
            pos_uncertainty=p.pos_uncertainty, sensor_used=p.sensor_used,
        ))


def _publish_position_event(pub: ThreadedPublisher, pet_id: str, r: PositionRecord) -> None:
    """Unretained one-shot publish to `tractive/<pid>/events/position` —
    consumed by telegraf into the `tractive_event` InfluxDB measurement.
    Each record carries its own `ts` so back-fill events land at the
    right timestamp."""
    if r.time_epoch is None or r.latitude is None or r.longitude is None:
        return
    payload = {
        "ts": dt.datetime.fromtimestamp(r.time_epoch, tz=dt.timezone.utc).isoformat(),
        "latitude": r.latitude,
        "longitude": r.longitude,
        "altitude": r.altitude,
        "speed": r.speed,
        "course": r.course,
        "gps_accuracy": r.pos_uncertainty,
        "sensor_used": r.sensor_used,
    }
    # Use publish_event (qos=1, acked) instead of publish_raw (qos=0) so a
    # backfill burst of 200+ position records doesn't silently lose tail
    # messages when paho's internal queue can't drain fast enough.
    pub.publish_event(
        f"{TOPIC_PREFIX}/{pet_id}/events/position",
        payload, retain=False,
    )


def _camel_to_snake(s: str) -> str:
    out = []
    for i, c in enumerate(s):
        if c.isupper() and i > 0:
            out.append("_")
        out.append(c.lower())
    return "".join(out)


def publish_timeline_event(pub: ThreadedPublisher, pet_id: str, evt: TimelineEvent) -> None:
    """Per-event publish: one-shot JSON event for InfluxDB ingest +
    retained `last_<type>_*` state mirrors for HA. Also flips the
    `inside_safezone` / `inside_dangerzone` derived binaries for
    enter/exit pairs."""
    if not evt.type:
        return
    base = f"{TOPIC_PREFIX}/{pet_id}"
    snake = _camel_to_snake(evt.type)
    ts_iso = evt.event_ts_iso

    # One-shot event for InfluxDB.
    if ts_iso:
        event_payload = {
            "ts": ts_iso,
            "type": evt.type,
            "id": evt.id,
        }
        if evt.zone_name is not None:
            event_payload["zone_name"] = evt.zone_name
        if evt.zone_category is not None:
            event_payload["zone_category"] = evt.zone_category
        if evt.duration_seconds is not None:
            event_payload["duration_seconds"] = evt.duration_seconds
        if evt.started_at:
            event_payload["started_at"] = evt.started_at
        if evt.ended_at:
            event_payload["ended_at"] = evt.ended_at
        pub.publish_event(
            f"{base}/events/{snake}", event_payload, retain=False,
        )

    # Retained `last_<type>_*` state mirrors for HA dashboards.
    if ts_iso:
        pub.publish_state(f"{base}/last_{snake}_at", ts_iso)
    if evt.type in ZONE_EVENT_TYPES and evt.zone_name:
        pub.publish_state(f"{base}/last_{snake}_zone", evt.zone_name)
    if evt.type in PERIOD_EVENT_TYPES and evt.duration_seconds is not None:
        pub.publish_state(f"{base}/last_{snake}_duration_s", str(evt.duration_seconds))

    # Derived "currently inside" binaries from the most-recent enter/exit
    # we've seen. Ordering note: the caller emits events in chronological
    # order, so flipping ON/OFF here ends up at the right final state.
    if evt.type == "safezoneEnter":
        pub.publish_state(f"{base}/inside_safezone", "ON")
    elif evt.type == "safezoneExit":
        pub.publish_state(f"{base}/inside_safezone", "OFF")
    elif evt.type == "dangerzoneEnter":
        pub.publish_state(f"{base}/inside_dangerzone", "ON")
    elif evt.type == "dangerzoneExit":
        pub.publish_state(f"{base}/inside_dangerzone", "OFF")


def backfill_positions(pub: ThreadedPublisher, pet: Pet, token: str,
                       hours: int, log: logging.Logger) -> int:
    """Fetch the last `hours` of positions and emit one event per record
    so the entire track lands in InfluxDB on bridge startup."""
    if hours <= 0:
        return 0
    now_ep = int(time.time())
    raw = fetch_positions(pet.device_id, token, now_ep - hours * 3600, now_ep)
    records = parse_positions(raw)
    for r in records:
        _publish_position_event(pub, pet.pet_id, r)
    if records:
        log.info("position backfill: %d records over last %dh", len(records), hours)
    return len(records)


def publish_activity_day(pub: ThreadedPublisher, pet_id: str, a: ActivityDayOverview) -> None:
    base = f"{TOPIC_PREFIX}/{pet_id}"
    if a.minutes_active is not None:
        pub.publish_state(f"{base}/activity_minutes_today", str(a.minutes_active))
    if a.minutes_goal is not None:
        pub.publish_state(f"{base}/activity_minutes_goal_today", str(a.minutes_goal))
    if a.minutes_active is not None and a.minutes_goal:
        pct = round(a.minutes_active / a.minutes_goal * 100)
        pub.publish_state(f"{base}/activity_goal_pct_today", str(pct))
    if a.calories is not None:
        pub.publish_state(f"{base}/activity_calories_today", str(a.calories))
    if a.current_streak is not None:
        pub.publish_state(f"{base}/activity_streak_days", str(a.current_streak))
    if a.breed_avg_minutes is not None:
        pub.publish_state(f"{base}/activity_breed_avg_minutes", str(a.breed_avg_minutes))
    if a.breed_deviation_pct is not None:
        pub.publish_state(f"{base}/activity_breed_deviation_pct", str(a.breed_deviation_pct))
    if a.age_group:
        pub.publish_state(f"{base}/activity_age_group", _humanize_enum(a.age_group))


def publish_activity_week(pub: ThreadedPublisher, pet_id: str, w: ActivityWeekOverview) -> None:
    # Week-overview entities aren't exposed individually — the data lives in
    # the discovery payload's `lookbackMinutes` attrs-future, plus we surface
    # `activity_streak_days` via the day-overview already. Reserved here for
    # later expansion into per-day-of-week sensors if useful.
    pass


def publish_weekly_report(pub: ThreadedPublisher, pet_id: str,
                          s: WeeklyReportSummary, d: WeeklyReportData) -> None:
    base = f"{TOPIC_PREFIX}/{pet_id}"
    if s.summary_text:
        # HA caps state at 255 chars — truncate just like the macOS bridge does.
        text = s.summary_text
        if len(text) > 252:
            text = text[:252] + "…"
        pub.publish_state(f"{base}/weekly_summary_text", text)
    if s.period_from:
        pub.publish_state(f"{base}/weekly_period_from", s.period_from)
    if s.period_to:
        pub.publish_state(f"{base}/weekly_period_to", s.period_to)
    if d.activity_avg_minutes is not None:
        pub.publish_state(f"{base}/activity_avg_minutes_7d", str(d.activity_avg_minutes))
    if d.calories_avg is not None:
        pub.publish_state(f"{base}/calories_avg_7d", str(d.calories_avg))
    if d.sleep_avg_minutes is not None:
        pub.publish_state(f"{base}/sleep_avg_minutes_7d", str(d.sleep_avg_minutes))
    if d.sleep_avg_interruptions is not None:
        pub.publish_state(f"{base}/sleep_interruptions_avg_7d", str(d.sleep_avg_interruptions))
    if d.vitality_avg_resting_hr_bpm is not None:
        pub.publish_state(f"{base}/resting_heart_rate_bpm", str(d.vitality_avg_resting_hr_bpm))
    if d.vitality_avg_resting_rr_brpm is not None:
        pub.publish_state(f"{base}/resting_respiratory_rate_brpm", str(d.vitality_avg_resting_rr_brpm))


def publish_health_monitors(pub: ThreadedPublisher, pet_id: str, s: HealthAlertsStatus) -> None:
    base = f"{TOPIC_PREFIX}/{pet_id}"
    for mtype in HEALTH_MONITOR_TYPES:
        status = s.monitors.get(mtype)
        if status is None:
            continue
        slug = f"monitor_{mtype.lower()}"
        pub.publish_state(f"{base}/{slug}_status", _humanize_enum(status))
        alert = "OFF" if status in HEALTH_MONITOR_HEALTHY else "ON"
        pub.publish_state(f"{base}/{slug}_alert", alert)


def publish_health_alerts_history(pub: ThreadedPublisher, pet_id: str, h: HealthAlertsHistory) -> None:
    base = f"{TOPIC_PREFIX}/{pet_id}"
    pub.publish_state(f"{base}/health_alerts_history_count", str(h.total))
    if h.last_type:
        pub.publish_state(f"{base}/last_health_alert_type", _humanize_enum(h.last_type))
    if h.last_detected_at:
        pub.publish_state(f"{base}/last_health_alert_detected_at", h.last_detected_at)
    if h.last_seen_at:
        pub.publish_state(f"{base}/last_health_alert_seen_at", h.last_seen_at)


def publish_bark_day(pub: ThreadedPublisher, pet_id: str, b: BarkDayOverview) -> None:
    base = f"{TOPIC_PREFIX}/{pet_id}"
    if b.average is not None:
        pub.publish_state(f"{base}/bark_count_today", str(b.average))
    if b.bound_upper is not None:
        pub.publish_state(f"{base}/bark_count_upper_bound", str(b.bound_upper))


def publish_scratch_day(pub: ThreadedPublisher, pet_id: str, s: ScratchDayOverview) -> None:
    base = f"{TOPIC_PREFIX}/{pet_id}"
    if s.seconds_scratch is not None:
        pub.publish_state(f"{base}/scratch_seconds_today", str(s.seconds_scratch))
    if s.events is not None:
        pub.publish_state(f"{base}/scratch_events_today", str(s.events))
    if s.bound_upper is not None:
        pub.publish_state(f"{base}/scratch_seconds_upper_bound", str(s.bound_upper))


def publish_separation(pub: ThreadedPublisher, pet_id: str, sep: SeparationPhases) -> None:
    base = f"{TOPIC_PREFIX}/{pet_id}"
    pub.publish_state(f"{base}/separation_ongoing", "ON" if sep.ongoing_count > 0 else "OFF")
    pub.publish_state(f"{base}/separation_ongoing_count", str(sep.ongoing_count))


def publish_account(pub: ThreadedPublisher, u: UserAccount) -> None:
    base = f"{TOPIC_PREFIX}/account"
    if u.email:
        pub.publish_state(f"{base}/account_email", u.email)
    if u.first_name:
        pub.publish_state(f"{base}/account_first_name", u.first_name)
    if u.country:
        pub.publish_state(f"{base}/account_country", u.country)
    if u.locale:
        pub.publish_state(f"{base}/account_locale", u.locale)
    if u.distance_unit:
        pub.publish_state(f"{base}/account_distance_unit", _humanize_enum(u.distance_unit))
    if u.weight_unit:
        pub.publish_state(f"{base}/account_weight_unit", _humanize_enum(u.weight_unit))
    if u.activated_at is not None:
        # Account epoch → ISO timestamp for the HA timestamp device_class.
        ts = dt.datetime.fromtimestamp(u.activated_at, tz=dt.timezone.utc).isoformat()
        pub.publish_state(f"{base}/account_activated_at", ts)


def publish_notifications(pub: ThreadedPublisher, n: UserNotifications) -> None:
    pub.publish_state(f"{TOPIC_PREFIX}/account/notifications_count", str(n.count))


def publish_shares(pub: ThreadedPublisher, s: UserShares) -> None:
    pub.publish_state(f"{TOPIC_PREFIX}/account/shares_count", str(s.count))


def publish_subscriptions(pub: ThreadedPublisher, s: Subscriptions) -> None:
    base = f"{TOPIC_PREFIX}/account"
    pub.publish_state(f"{base}/subscriptions_count", str(s.count))
    pub.publish_state(f"{base}/subscription_active", "ON" if s.count > 0 else "OFF")


# -------------------------------------------------------------- main


def _handle_channel_message(pub: ThreadedPublisher,
                             device_to_pet: dict[str, Pet],
                             msg: dict, log: logging.Logger) -> None:
    """Dispatch a push-channel frame into the existing publish helpers.

    Channel frames are sparse — most just carry the fields that changed.
    We map the documented shapes (tracker_status / position / hw_info) to
    PosReport / TrackerState / HwReport so the same code paths publish
    the same retained-state topics regardless of whether the trigger was
    a FAST-poll REST call or a push event.

    Multi-pet routing: frames carry `tracker_id` (the device_id we
    discovered at startup). We look up the matching pet and publish
    under its topic namespace.

    Untyped frames are ignored — Tractive occasionally emits internal
    bookkeeping messages and we don't want to thrash MQTT for them.
    """
    tid = msg.get("tracker_id")
    if not tid:
        return
    pet = device_to_pet.get(tid)
    if pet is None:
        # Frame for a tracker we don't know about — likely a sibling pet
        # that joined the account after the bridge started. Restart picks
        # it up.
        return

    mtype = msg.get("message") or msg.get("type")
    if not mtype:
        return

    if mtype == "tracker_status":
        # Channel emits a sparse subset of the /4/tracker/{id} blob.
        publish_tracker(pub, pet.pet_id, parse_tracker(msg))
    elif mtype == "hw_info":
        publish_hw(pub, pet.pet_id, parse_hw_report(msg))
    elif mtype == "position":
        # Channel position frames have the same field names as
        # /4/device_pos_report (latlong, sensor_used, pos_uncertainty,
        # speed, altitude, time).
        publish_pos(pub, pet.pet_id, parse_pos_report(msg))
    elif mtype == "health_overview":
        publish_health(pub, pet.pet_id, parse_health_overview(msg))


def reconnect_channel_if_stale(
    channel: ChannelClient | None,
    make_channel,
    log: logging.Logger,
) -> ChannelClient | None:
    """If the push channel has gone stale, tear it down and start a
    fresh one via `make_channel()`.

    `ChannelClient.stale` existed but nothing ever read it: the
    channel's own internal reconnect loop only fires on a genuine
    socket-level disconnect or read timeout, which doesn't cover a
    connection that is technically still open (bytes still arriving —
    HTTP chunk boundaries, TCP keepalives) but has stopped delivering
    any actual message for longer than `CHANNEL_STALE_AFTER`. This is
    the periodic, main-loop-driven check that closes that gap.

    `channel is None` (channel disabled, or not started yet) is a no-op
    — returns `channel` unchanged. A non-stale channel is likewise
    returned unchanged, so callers can unconditionally reassign their
    local variable to this function's return value every loop
    iteration.
    """
    if channel is None or not channel.stale:
        return channel
    log.warning("push channel stale; reconnecting")
    channel.stop()
    new_channel = make_channel()
    new_channel.start()
    return new_channel


def discover_pets(token: str, log: logging.Logger) -> list[Pet]:
    """Resolve every pet on the account. The bridge iterates this list
    across all tier handlers, so adding a second tracker to the Tractive
    account is a hot-swap: restart the bridge and the new pet appears as
    its own HA device with the full Phase-1/2/3/4/5 entity surface."""
    objs = fetch_trackable_objects(token)
    if not objs:
        log.warning("no trackable_objects on account")
        return []
    out: list[Pet] = []
    for obj in objs:
        obj_id = obj.get("_id")
        if not obj_id:
            continue
        time.sleep(INTER_CALL_DELAY)
        pet_payload = fetch_pet(obj_id, token)
        details = pet_payload.get("details") or {}
        out.append(Pet(
            pet_id=obj_id,
            name=details.get("name") or obj_id,
            device_id=pet_payload.get("device_id") or "",
            breed_ids=details.get("breed_ids") or [],
        ))
    return out


# --- Tiered cycle implementations --------------------------------------


def _today_iso() -> str:
    return dt.date.today().isoformat()


def run_fast_cycle(pub, pet, token, log) -> None:
    """Phase-1 endpoints — biometric statuses + tracker + position."""
    tracker = parse_tracker(fetch_tracker(pet.device_id, token))
    publish_tracker(pub, pet.pet_id, tracker)
    time.sleep(INTER_CALL_DELAY)

    hw = parse_hw_report(fetch_hw_report(pet.device_id, token))
    publish_hw(pub, pet.pet_id, hw)
    time.sleep(INTER_CALL_DELAY)

    pos = parse_pos_report(fetch_pos_report(pet.device_id, token))
    publish_pos(pub, pet.pet_id, pos)
    time.sleep(INTER_CALL_DELAY)

    health = parse_health_overview(fetch_health_overview(pet.pet_id, token))
    publish_health(pub, pet.pet_id, health)

    log.debug("fast cycle ok bark=%s scratch=%s alerts=%d state=%s",
              health.bark_status, health.scratch_status,
              health.unseen_health_alerts, tracker.state_reason)


def run_med_cycle(pub, pet, token, log, event_stream: EventStream | None = None,
                  current_token_holder=None) -> None:
    """5-minute tier — daily activity, bark/scratch counts, health monitors,
    separation, notifications."""
    today = _today_iso()

    day = parse_activity_day_overview(fetch_activity_day_overview(pet.pet_id, token, today))
    publish_activity_day(pub, pet.pet_id, day)
    time.sleep(INTER_CALL_DELAY)

    bark = parse_bark_day_overview(fetch_bark_day_overview(pet.pet_id, token, today))
    publish_bark_day(pub, pet.pet_id, bark)
    time.sleep(INTER_CALL_DELAY)

    scratch = parse_scratch_day_overview(fetch_scratch_day_overview(pet.pet_id, token, today))
    publish_scratch_day(pub, pet.pet_id, scratch)
    time.sleep(INTER_CALL_DELAY)

    monitors = parse_health_alerts_status(fetch_health_alerts_status(pet.pet_id, token))
    publish_health_monitors(pub, pet.pet_id, monitors)
    time.sleep(INTER_CALL_DELAY)

    history = parse_health_alerts_history(fetch_health_alerts_history(pet.pet_id, token))
    publish_health_alerts_history(pub, pet.pet_id, history)
    time.sleep(INTER_CALL_DELAY)

    sep = parse_separation_phases(fetch_separation_phases_ongoing(pet.pet_id, token))
    publish_separation(pub, pet.pet_id, sep)
    time.sleep(INTER_CALL_DELAY)

    notif = parse_user_notifications(fetch_user_notifications(token))
    publish_notifications(pub, notif)

    # Phase 4/5 — incremental poll of the event-timeline. We're already
    # holding the rate-limit-friendly INTER_CALL_DELAY budget from the
    # previous endpoints, so a single page-1 fetch here is safe.
    new_events = 0
    if event_stream is not None:
        time.sleep(INTER_CALL_DELAY)
        new_events = event_stream.poll_incremental(
            token, _auth_headers, TIMELINE_EVENT_TYPES,
            lambda evt: publish_timeline_event(pub, pet.pet_id, evt), log,
        )

    log.debug("med cycle ok active=%s/%s cal=%s bark=%s scratch_s=%s monitors=%d sep=%d new_events=%d",
              day.minutes_active, day.minutes_goal, day.calories,
              bark.average, scratch.seconds_scratch,
              len(monitors.monitors), sep.ongoing_count, new_events)


def run_slow_pet_cycle(pub, pet, token, log) -> None:
    """Hourly per-pet tier — weekly report + activity week overview."""
    today = _today_iso()

    week = parse_activity_week_overview(fetch_activity_week_overview(pet.pet_id, token, today))
    publish_activity_week(pub, pet.pet_id, week)
    time.sleep(INTER_CALL_DELAY)

    wr_summary = parse_weekly_report_summary(fetch_weekly_report_summary(pet.pet_id, token))
    time.sleep(INTER_CALL_DELAY)
    wr_data = parse_weekly_report_data(fetch_weekly_report_data(pet.pet_id, token))
    publish_weekly_report(pub, pet.pet_id, wr_summary, wr_data)

    log.debug("slow pet=%s ok rhr=%s rr=%s sleep_min=%s",
              pet.pet_id, wr_data.vitality_avg_resting_hr_bpm,
              wr_data.vitality_avg_resting_rr_brpm, wr_data.sleep_avg_minutes)


def run_slow_account_cycle(pub, token, log) -> None:
    """Hourly account-scoped tier — user record, shares, subscriptions.

    Runs once per slow cycle regardless of pet count (subscription state
    is account-wide, not per-pet)."""
    user = parse_user_account(fetch_user(token))
    publish_account(pub, user)
    time.sleep(INTER_CALL_DELAY)

    shares = parse_user_shares(fetch_user_shares(token))
    publish_shares(pub, shares)
    time.sleep(INTER_CALL_DELAY)

    subs_raw = fetch_user_subscriptions(token)
    time.sleep(INTER_CALL_DELAY)
    usage_raw = fetch_user_subscriptions_usage(token)
    subs = parse_subscriptions(subs_raw, usage_raw)
    publish_subscriptions(pub, subs)

    log.debug("slow account ok subs=%d shares=%d", subs.count, shares.count)


def main() -> int:
    log = configure_logging("tractive-mqtt-bridge", LOG_LEVEL)
    log.info("starting; fast=%ss med=%ss slow=%ss inter_call=%ss",
             FAST_POLL, MED_POLL, SLOW_POLL, INTER_CALL_DELAY)

    access, expires_at = auth_token()
    log.info("login ok; token expires in %ds", expires_at - int(time.time()))

    pub = ThreadedPublisher(
        host=MQTT_HOST, port=MQTT_PORT, username=MQTT_USER, password=MQTT_PASS,
        client_id=f"tractive-mqtt-bridge-{uuid.uuid4().hex[:8]}",
        lwt_topic=BRIDGE_LWT_TOPIC, discovery_prefix=DISCOVERY_PREFIX,
        health_path="/tmp/healthy", tls=MQTT_TLS, ca_file=MQTT_CA_FILE,
    )
    pub.start()

    pets: list[Pet] = []
    device_to_pet: dict[str, Pet] = {}
    event_streams: dict[str, EventStream] = {}
    discovery_published = False
    backfill_done = False
    stopping = False
    channel: ChannelClient | None = None
    # `current_token` holds the live access token in a one-element list so
    # the channel thread (which auths once per session) re-reads it after
    # the main loop refreshes the token. Plain str variable wouldn't be
    # visible to the closure after rebind.
    current_token: list[str] = [access]
    # next_run_at per tier; 0 means "run on next iteration".
    next_fast = 0.0
    next_med = 0.0
    next_slow = 0.0

    def on_signal(signum, _frame):
        nonlocal stopping
        log.info("signal %s, shutting down", signum)
        stopping = True
    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    def make_channel() -> ChannelClient:
        return ChannelClient(
            auth_headers_fn=lambda: _auth_headers(current_token[0]),
            on_message=lambda msg: _handle_channel_message(
                pub, device_to_pet, msg, log,
            ),
            log=log,
        )

    def _on_ha_birth() -> None:
        # HA republishes nothing on its own restart; retained discovery
        # configs usually survive in the broker, but not always. Re-run
        # discovery and nudge every tier to run on the next loop
        # iteration so current state follows quickly behind.
        nonlocal discovery_published, next_fast, next_med, next_slow
        log.info("HA birth message received; re-publishing discovery and refreshing state")
        discovery_published = False
        next_fast = 0.0
        next_med = 0.0
        next_slow = 0.0

    watch_ha_birth(pub, _on_ha_birth, discovery_prefix=DISCOVERY_PREFIX)

    while not stopping:
        try:
            # Refresh token an hour before expiry
            if int(time.time()) > expires_at - 3600:
                access, expires_at = auth_token()
                current_token[0] = access
                log.info("token refreshed; expires in %ds", expires_at - int(time.time()))

            if not pets:
                pets = discover_pets(access, log)
                if not pets:
                    log.warning("no pets found, retrying in %ds", FAST_POLL)
                    time.sleep(FAST_POLL)
                    continue
                for p in pets:
                    device_to_pet[p.device_id] = p
                log.info("discovered %d pet(s): %s", len(pets),
                         ", ".join(f"{p.name}({p.pet_id[-6:]})" for p in pets))

            if not discovery_published:
                # Per-pet entities — one device per pet under HA.
                total = 0
                for p in pets:
                    for component, unique_id, payload in discovery_specs_pet(
                        p, TOPIC_PREFIX, BRIDGE_LWT_TOPIC
                    ):
                        pub.publish_discovery(component=component, unique_id=unique_id, payload=payload)
                        total += 1
                # Account-scoped entities — once per bridge instance.
                for component, unique_id, payload in discovery_specs_account(
                    TOPIC_PREFIX, BRIDGE_LWT_TOPIC
                ):
                    pub.publish_discovery(component=component, unique_id=unique_id, payload=payload)
                    total += 1
                discovery_published = True
                log.info("discovery published: %d entities across %d pet(s) + account",
                         total, len(pets))

            if not backfill_done:
                # Per-pet backfills + event streams. Independent of
                # discovery_published so an HA-birth re-publish of
                # discovery doesn't also re-run a full history backfill
                # and restart an already-healthy push channel.
                for p in pets:
                    try:
                        backfill_positions(pub, p, access, POSITION_BACKFILL_HOURS, log)
                    except Exception:
                        log.exception("position backfill failed for %s (non-fatal)", p.name)
                    stream = EventStream(p.pet_id, timezone_name=EVENT_TIMEZONE)
                    stream.backfill(
                        access, _auth_headers, TIMELINE_EVENT_TYPES,
                        EVENT_BACKFILL_DAYS,
                        lambda evt, pid=p.pet_id: publish_timeline_event(pub, pid, evt),
                        log,
                    )
                    event_streams[p.pet_id] = stream

                # Phase 4 — single push channel for the account; routes
                # messages to the right pet by tracker_id at dispatch time.
                if CHANNEL_ENABLED:
                    channel = make_channel()
                    channel.start()
                backfill_done = True

            channel = reconnect_channel_if_stale(channel, make_channel, log)

            now = time.monotonic()
            if now >= next_fast:
                for p in pets:
                    try:
                        run_fast_cycle(pub, p, access, log)
                    except PermissionError:
                        raise
                    except Exception as e:
                        log.exception("fast cycle failed for %s: %s", p.name, e)
                next_fast = now + FAST_POLL

            if now >= next_med:
                for p in pets:
                    try:
                        run_med_cycle(pub, p, access, log,
                                      event_stream=event_streams.get(p.pet_id))
                    except PermissionError:
                        raise
                    except Exception as e:
                        log.exception("med cycle failed for %s: %s", p.name, e)
                next_med = now + MED_POLL

            if now >= next_slow:
                for p in pets:
                    try:
                        run_slow_pet_cycle(pub, p, access, log)
                    except PermissionError:
                        raise
                    except Exception as e:
                        log.exception("slow pet cycle failed for %s: %s", p.name, e)
                try:
                    run_slow_account_cycle(pub, access, log)
                except PermissionError:
                    raise
                except Exception as e:
                    log.exception("slow account cycle failed: %s", e)
                next_slow = now + SLOW_POLL

        except PermissionError:
            log.warning("auth expired mid-poll; re-authing")
            try:
                access, expires_at = auth_token()
            except Exception as e:
                log.error("re-auth failed: %s", e)
                time.sleep(30)
        except requests.RequestException as e:
            log.error("network/HTTP error: %s", e)
        except Exception:
            # Last-resort guard for the main loop itself — e.g. a
            # RetryExhaustedError surfacing from pet/tracker discovery,
            # which (unlike the FAST/MED/SLOW tier runners) isn't wrapped
            # in its own per-call try/except. Previously this class of
            # error propagated straight out of `main()` and killed the
            # process.
            log.exception("poll cycle failed unexpectedly")

        # Sleep until the soonest next-tier deadline, capped at 1 s
        # granularity so SIGTERM stays responsive.
        for _ in range(5):
            if stopping:
                break
            time.sleep(1)

    if channel is not None:
        channel.stop()
    pub.stop()
    log.info("shutdown clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
