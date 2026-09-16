"""Pure-data layer for the Tractive bridge.

Holds the dataclass shapes consumed by every other module, the parse_*
functions that turn raw Tractive JSON into those dataclasses, and the
small derivation helpers (enum humanizer, position-accuracy bucket).

No network I/O, no MQTT, no env vars — keeps the parsing layer trivially
unit-testable from a fixture file.

Payload shapes were live-probed against the API on 2026-05-26.
"""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass, field


def _obj(payload) -> dict:
    """Normalise an absent Tractive payload to an empty dict.

    `main._request` returns None for any empty response body, and says of it
    "let the parsers normalise". Only parse_user_notifications/_shares ever
    did. Tractive answers 200-with-no-body whenever a resource exists but has
    no data yet -- a tracker that has not reported a position, a pet with no
    health record -- so every dict parser must tolerate it. Before this, one
    such endpoint (device_pos_report for a tracker that was not reporting)
    crashed the whole per-pet fast cycle every 90 s with
    "'NoneType' object has no attribute 'get'", taking the pet's other
    metrics down with it.
    """
    return payload if isinstance(payload, dict) else {}


# Treat these statuses as healthy baseline; anything else trips the binary_sensor.
# NOT_SYNCED_YET fires when the tracker hasn't pushed fresh VEDBA data — happens
# whenever the tracker is idle inside a known Wi-Fi zone (overnight at home,
# typically). It is NOT a health concern, so it stays in the
# healthy set. We only alert on positively-bad enums.
HEALTHY_STATUSES = {"NORMAL", "NOT_SYNCED_YET"}
SCRATCH_HEALTHY = {"NORMAL", "INFREQUENT", "NOT_SYNCED_YET"}

# Health-monitor `alertStatus` values considered "fine". Anything else (e.g.
# TRIGGERED, DEGRADED) trips a problem binary_sensor for that monitor.
HEALTH_MONITOR_HEALTHY = {"MONITORED", "NOT_ENOUGH_DATA"}

# Substring overrides for the title-case helper — restored AFTER `.title()`
# normalises the raw enum. Keys are the `.title()` form, values are the
# canonical capitalisation we want HA to display.
_TITLE_OVERRIDES = {
    "Wifi": "Wi-Fi",
    "Gps": "GPS",
    "Ble": "BLE",
    "Lte": "LTE",
    "Usb": "USB",
}


def _humanize_enum(value):
    """Normalize an UPPER_SNAKE / lowercase enum to display-friendly Title Case.

    Underscores → spaces; hyphens preserved (`BLACK-LINES` → `Black-Lines`);
    known acronyms restored via the overrides table. Returns the input as-is
    if it's None, empty, or already a non-enum-shaped string (we keep
    firmware versions like `010.083-TRV6-014-ab8d9f` untouched by detecting a
    period in the value — enums never contain periods).
    """
    if value is None or value == "" or not isinstance(value, str):
        return value
    if "." in value:
        return value
    s = value.replace("_", " ").lower().title()
    for k, v in _TITLE_OVERRIDES.items():
        s = re.sub(rf"(^|[\s\-]){re.escape(k)}($|[\s\-])", rf"\1{v}\2", s)
    return s


def _pos_accuracy_quality(uncertainty_m):
    """Derive a human-readable position-accuracy bucket from
    `pos_uncertainty` (meters; lower is better).

    Calibrated against observed Tractive values: GPS lock outdoors reports
    ~5-10 m, Known-Wi-Fi zone reports ~30 m, cell-tower fallback reports
    100+ m. The buckets are deliberately wide because the underlying signal
    is itself an estimate.
    """
    if uncertainty_m is None:
        return None
    try:
        u = float(uncertainty_m)
    except (TypeError, ValueError):
        return None
    if u <= 5:
        return "Excellent"
    if u <= 15:
        return "Good"
    if u <= 50:
        return "Fair"
    return "Poor"


@dataclass
class Pet:
    pet_id: str
    name: str
    device_id: str
    breed_ids: list[str]


@dataclass
class HealthOverview:
    activity_minutes_active: int | None = None
    activity_minutes_goal: int | None = None
    sleep_minutes_day: int | None = None
    sleep_minutes_night: int | None = None
    sleep_minutes_calm: int | None = None
    bark_status: str | None = None
    heart_rate_status: str | None = None
    respiratory_rate_status: str | None = None
    scratch_status: str | None = None
    unseen_health_alerts: int = 0
    data_synced_at: str | None = None


@dataclass
class TrackerState:
    fw_version: str | None = None
    model_number: str | None = None
    hw_edition: str | None = None
    state: str | None = None
    state_reason: str | None = None
    battery_state: str | None = None
    charging_state: str | None = None
    capabilities: list[str] = field(default_factory=list)


@dataclass
class HwReport:
    battery_level: int | None = None
    clip_mounted_state: str | None = None
    temperature_state: str | None = None


@dataclass
class PosReport:
    sensor_used: str | None = None
    pos_uncertainty: int | None = None
    speed: float | None = None
    altitude: float | None = None
    latlong: list[float] | None = None
    time: int | None = None


# ---------------------- Phase 2 dataclasses ----------------------


@dataclass
class ActivityDayOverview:
    """`GET /api/2/pet/{pet_id}/activity/day-overview?date=`"""
    minutes_active: int | None = None
    minutes_goal: int | None = None
    average_minutes_active: int | None = None
    current_streak: int | None = None
    calories: int | None = None
    hourly_distribution: list[int] = field(default_factory=list)
    lookback_minutes: list[dict] = field(default_factory=list)
    breed_avg_minutes: int | None = None
    breed_deviation_pct: int | None = None
    age_group: str | None = None


@dataclass
class ActivityWeekOverview:
    """`GET /api/1/pet/{pet_id}/activity/week-overview?date=`"""
    days: list[dict] = field(default_factory=list)  # [{date,progress,streakAfter,streakBefore}]


@dataclass
class WeeklyReportSummary:
    """`GET /api/1/pet/{pet_id}/health/weekly-report/summary`"""
    period_from: str | None = None
    period_to: str | None = None
    summary_text: str | None = None


@dataclass
class WeeklyReportData:
    """`GET /api/1/pet/{pet_id}/health/weekly-report/data`"""
    period_from: str | None = None
    period_to: str | None = None
    activity_avg_minutes: int | None = None
    calories_avg: int | None = None
    sleep_avg_minutes: int | None = None
    sleep_avg_interruptions: int | None = None
    sleep_avg_phase_duration_minutes: int | None = None
    vitality_avg_resting_hr_bpm: int | None = None
    vitality_avg_resting_rr_brpm: int | None = None


@dataclass
class HealthAlertsStatus:
    """`GET /api/1/pet/{pet_id}/health-alerts/status`

    `monitors` is a dict[alert_type, alert_status] for ergonomic lookup;
    `raw` is the original list preserved for json_attributes.
    """
    monitors: dict[str, str] = field(default_factory=dict)
    raw: list[dict] = field(default_factory=list)


@dataclass
class HealthAlertsHistory:
    """`GET /api/1/pet/{pet_id}/health-alerts/history`"""
    total: int = 0
    last_id: str | None = None
    last_type: str | None = None
    last_detected_at: str | None = None
    last_seen_at: str | None = None


@dataclass
class BarkDayOverview:
    """`GET /api/1/pet/{pet_id}/bark/day-overview?date=`"""
    status: str | None = None
    average: int | None = None          # today's bark count
    bound_min: int | None = None
    bound_lower: int | None = None
    bound_upper: int | None = None
    bound_max: int | None = None
    age_group: str | None = None


@dataclass
class ScratchDayOverview:
    """`GET /api/1/pet/{pet_id}/scratch/day-overview?date=`"""
    status: str | None = None
    seconds_scratch: int | None = None  # today's seconds-spent-scratching
    bound_min: int | None = None
    bound_lower: int | None = None
    bound_upper: int | None = None
    bound_max: int | None = None
    events: int | None = None
    lookback: list[dict] = field(default_factory=list)


@dataclass
class SeparationPhases:
    """`GET /api/1/pet/{pet_id}/separation/phases?states=ONGOING`"""
    ongoing_count: int = 0
    raw: list[dict] = field(default_factory=list)


@dataclass
class UserAccount:
    """`GET /4/user/{uid}`"""
    email: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    country: str | None = None
    language: str | None = None
    locale: str | None = None
    distance_unit: str | None = None
    weight_unit: str | None = None
    metric_system: bool | None = None
    profile_picture_id: str | None = None
    activated_at: int | None = None


@dataclass
class UserNotifications:
    """`GET /4/user/{uid}/notifications`

    Tractive returns the literal empty string `""` when there are no
    notifications, otherwise a list. Both shapes collapse to a count.
    """
    count: int = 0
    raw: list[dict] = field(default_factory=list)


@dataclass
class UserShares:
    """`GET /4/user/{uid}/shares`"""
    count: int = 0
    raw: list[dict] = field(default_factory=list)


@dataclass
class Subscriptions:
    """`GET /4/user/{uid}/subscriptions` (+ `/usage`)

    The list-form response only returns `{_id, _type, _version}` stubs;
    plan name + renewal date would need a per-id follow-up GET we have
    not wired in. For now we expose count + active boolean which is
    enough to flag "this account has a paid subscription".
    """
    count: int = 0
    usage_count: int = 0
    raw_subscriptions: list[dict] = field(default_factory=list)
    raw_usage: list[dict] = field(default_factory=list)


def parse_health_overview(payload: dict) -> HealthOverview:
    payload = _obj(payload)
    activity = payload.get("activity") or {}
    sleep = payload.get("sleep") or {}
    return HealthOverview(
        activity_minutes_active=activity.get("minutesActive"),
        activity_minutes_goal=activity.get("minutesGoal"),
        sleep_minutes_day=sleep.get("minutesDaySleep"),
        sleep_minutes_night=sleep.get("minutesNightSleep"),
        sleep_minutes_calm=sleep.get("minutesCalm"),
        bark_status=(payload.get("bark") or {}).get("status"),
        heart_rate_status=(payload.get("restingHeartRate") or {}).get("status"),
        respiratory_rate_status=(payload.get("restingRespiratoryRate") or {}).get("status"),
        scratch_status=(payload.get("scratch") or {}).get("status"),
        unseen_health_alerts=(payload.get("healthAlerts") or {}).get("unseenCount", 0),
        data_synced_at=payload.get("activityDataSyncedAt"),
    )


def parse_tracker(payload: dict) -> TrackerState:
    payload = _obj(payload)
    return TrackerState(
        fw_version=payload.get("fw_version"),
        model_number=payload.get("model_number"),
        hw_edition=payload.get("hw_edition"),
        state=payload.get("state"),
        state_reason=payload.get("state_reason"),
        battery_state=payload.get("battery_state"),
        charging_state=payload.get("charging_state"),
        capabilities=payload.get("capabilities") or [],
    )


def parse_hw_report(payload: dict) -> HwReport:
    payload = _obj(payload)
    return HwReport(
        battery_level=payload.get("battery_level"),
        clip_mounted_state=payload.get("clip_mounted_state"),
        temperature_state=payload.get("temperature_state"),
    )


def parse_pos_report(payload: dict) -> PosReport:
    payload = _obj(payload)
    return PosReport(
        sensor_used=payload.get("sensor_used"),
        pos_uncertainty=payload.get("pos_uncertainty"),
        speed=payload.get("speed"),
        altitude=payload.get("altitude"),
        latlong=payload.get("latlong"),
        time=payload.get("time"),
    )


# ---------------------- Phase 2 parse_* ----------------------


def parse_activity_day_overview(payload: dict) -> ActivityDayOverview:
    payload = _obj(payload)
    overview = payload.get("overview") or {}
    benchmarks = payload.get("benchmarks") or {}
    return ActivityDayOverview(
        minutes_active=overview.get("minutesActive"),
        minutes_goal=overview.get("minutesGoal"),
        average_minutes_active=overview.get("averageMinutesActive"),
        current_streak=overview.get("currentStreak"),
        calories=overview.get("calories"),
        hourly_distribution=payload.get("hourlyDistribution") or [],
        lookback_minutes=payload.get("lookbackMinutes") or [],
        breed_avg_minutes=benchmarks.get("averageMinutesActiveBreed"),
        breed_deviation_pct=benchmarks.get("deviationPercent"),
        age_group=benchmarks.get("ageGroup"),
    )


def parse_activity_week_overview(payload: dict) -> ActivityWeekOverview:
    payload = _obj(payload)
    return ActivityWeekOverview(days=payload.get("weekOverview") or [])


def parse_weekly_report_summary(payload: dict) -> WeeklyReportSummary:
    payload = _obj(payload)
    period = payload.get("period") or {}
    return WeeklyReportSummary(
        period_from=period.get("from"),
        period_to=period.get("to"),
        summary_text=payload.get("summary"),
    )


def parse_weekly_report_data(payload: dict) -> WeeklyReportData:
    payload = _obj(payload)
    period = payload.get("period") or {}
    activity = payload.get("activity") or {}
    calories = payload.get("calories") or {}
    sleep = payload.get("sleep") or {}
    vitality = payload.get("vitality") or {}
    return WeeklyReportData(
        period_from=period.get("from"),
        period_to=period.get("to"),
        activity_avg_minutes=activity.get("averageMinutes"),
        calories_avg=calories.get("average"),
        sleep_avg_minutes=sleep.get("averageMinutes"),
        sleep_avg_interruptions=sleep.get("averageInterruptions"),
        sleep_avg_phase_duration_minutes=sleep.get("averageSleepPhaseDurationMinutes"),
        vitality_avg_resting_hr_bpm=vitality.get("averageRestingHeartRate"),
        vitality_avg_resting_rr_brpm=vitality.get("averageRestingRespiratoryRate"),
    )


def parse_health_alerts_status(payload: dict) -> HealthAlertsStatus:
    payload = _obj(payload)
    raw = payload.get("healthMonitoringStatus") or []
    monitors = {
        item.get("alertType"): item.get("alertStatus")
        for item in raw if item.get("alertType")
    }
    return HealthAlertsStatus(monitors=monitors, raw=raw)


def parse_health_alerts_history(payload: dict) -> HealthAlertsHistory:
    payload = _obj(payload)
    alerts = payload.get("healthAlerts") or []
    if not alerts:
        return HealthAlertsHistory(total=0)
    # Tractive returns newest-first per observation. Trust that.
    last = alerts[0]
    return HealthAlertsHistory(
        total=len(alerts),
        last_id=last.get("id"),
        last_type=last.get("type"),
        last_detected_at=last.get("detectedAt"),
        last_seen_at=last.get("seenAt"),
    )


def parse_bark_day_overview(payload: dict) -> BarkDayOverview:
    payload = _obj(payload)
    bounds = payload.get("bounds") or {}
    return BarkDayOverview(
        status=payload.get("status"),
        average=payload.get("average"),
        bound_min=bounds.get("min"),
        bound_lower=bounds.get("lower"),
        bound_upper=bounds.get("upper"),
        bound_max=bounds.get("max"),
        age_group=payload.get("ageGroup"),
    )


def parse_scratch_day_overview(payload: dict) -> ScratchDayOverview:
    payload = _obj(payload)
    bounds = payload.get("bounds") or {}
    # `events` is a structured object `{"scratch": [{ts,duration}], "noData":
    # [...]}` — NOT an int. The count we want is the number of scratch
    # episodes detected today (len of events.scratch). `noData` reflects
    # bands of time where the tracker wasn't reporting; we don't surface
    # those as their own sensor today.
    events_obj = payload.get("events") or {}
    scratch_episodes = (events_obj.get("scratch") or []) if isinstance(events_obj, dict) else []
    return ScratchDayOverview(
        status=payload.get("status"),
        seconds_scratch=payload.get("secondsScratch"),
        bound_min=bounds.get("min"),
        bound_lower=bounds.get("lower"),
        bound_upper=bounds.get("upper"),
        bound_max=bounds.get("max"),
        events=len(scratch_episodes),
        lookback=payload.get("lookback") or [],
    )


def parse_separation_phases(payload: list) -> SeparationPhases:
    items = payload or []
    return SeparationPhases(ongoing_count=len(items), raw=items)


def parse_user_account(payload: dict) -> UserAccount:
    payload = _obj(payload)
    details = payload.get("details") or {}
    demographics = payload.get("demographics") or {}
    settings = payload.get("settings") or {}
    return UserAccount(
        email=payload.get("email"),
        first_name=details.get("first_name"),
        last_name=details.get("last_name"),
        country=demographics.get("country"),
        language=demographics.get("language"),
        locale=demographics.get("locale"),
        distance_unit=settings.get("distance_unit"),
        weight_unit=settings.get("weight_unit"),
        metric_system=settings.get("metric_system"),
        profile_picture_id=payload.get("profile_picture_id"),
        activated_at=payload.get("activated_at"),
    )


def parse_user_notifications(payload) -> UserNotifications:
    # Tractive returns the literal empty string when no notifications exist
    # (observed 2026-05-26). Otherwise a list of notification objects.
    if not payload or isinstance(payload, str):
        return UserNotifications(count=0)
    items = payload if isinstance(payload, list) else []
    return UserNotifications(count=len(items), raw=items)


def parse_user_shares(payload) -> UserShares:
    items = payload if isinstance(payload, list) else []
    return UserShares(count=len(items), raw=items)


# ---------------------- Position-history dataclasses ----------------------


@dataclass
class PositionRecord:
    """One row from `GET /4/tracker/{dev_id}/positions?format=json_segments`.

    Tractive groups records into "segments" (a list of position arrays),
    typically one segment per power-on cycle. We flatten into a flat list
    of records since downstream code doesn't care about segment grouping.
    """
    time_epoch: int | None = None
    latitude: float | None = None
    longitude: float | None = None
    altitude: float | None = None
    speed: float | None = None
    course: int | None = None
    pos_uncertainty: int | None = None
    sensor_used: str | None = None


# ---------------------- Timeline-event dataclass ----------------------


@dataclass
class TimelineEvent:
    """One row from the event-timeline API.

    Instantaneous events (e.g. safezoneEnter, dailyActivityGoalReached)
    carry `occurred_at`. Period events (walk, insidePowerSavingZone)
    carry `started_at` + `ended_at` instead. `event_ts_iso` is the
    canonical timestamp used for InfluxDB and `last_<type>_at`
    sensors — `occurred_at` when present, otherwise `started_at`.
    """
    id: str
    type: str
    occurred_at: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    zone_name: str | None = None
    zone_category: str | None = None
    duration_seconds: int | None = None
    raw: dict = field(default_factory=dict)

    @property
    def event_ts_iso(self) -> str | None:
        return self.occurred_at or self.started_at


def parse_events(payload) -> list[TimelineEvent]:
    """Returns events in their original (newest-first) order."""
    if not isinstance(payload, dict):
        return []
    raw_events = payload.get("events") or []
    out: list[TimelineEvent] = []
    for r in raw_events:
        if not isinstance(r, dict):
            continue
        started = r.get("startedAt")
        ended = r.get("endedAt")
        duration = None
        if started and ended:
            try:
                s = dt.datetime.fromisoformat(started)
                e = dt.datetime.fromisoformat(ended)
                duration = int((e - s).total_seconds())
            except (TypeError, ValueError):
                duration = None
        out.append(TimelineEvent(
            id=r.get("id") or "",
            type=r.get("type") or "",
            occurred_at=r.get("occurredAt"),
            started_at=started,
            ended_at=ended,
            zone_name=r.get("zoneName"),
            zone_category=r.get("zoneCategory"),
            duration_seconds=duration,
            raw=r,
        ))
    return out


def parse_positions(payload) -> list[PositionRecord]:
    """Flatten the nested `list[segment][position]` shape into a single
    chronologically-ordered list of PositionRecord."""
    if not isinstance(payload, list):
        return []
    out: list[PositionRecord] = []
    for segment in payload:
        if not isinstance(segment, list):
            continue
        for raw in segment:
            if not isinstance(raw, dict):
                continue
            ll = raw.get("latlong") or [None, None]
            lat = ll[0] if len(ll) >= 1 else None
            lon = ll[1] if len(ll) >= 2 else None
            out.append(PositionRecord(
                time_epoch=raw.get("time"),
                latitude=lat,
                longitude=lon,
                altitude=raw.get("alt"),
                speed=raw.get("speed"),
                course=raw.get("course"),
                pos_uncertainty=raw.get("pos_uncertainty"),
                sensor_used=raw.get("sensor_used"),
            ))
    out.sort(key=lambda r: r.time_epoch or 0)
    return out


def parse_subscriptions(subs_payload, usage_payload) -> Subscriptions:
    subs = subs_payload if isinstance(subs_payload, list) else []
    usage = usage_payload if isinstance(usage_payload, list) else []
    return Subscriptions(
        count=len(subs),
        usage_count=len(usage),
        raw_subscriptions=subs,
        raw_usage=usage,
    )
