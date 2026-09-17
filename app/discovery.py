"""HA MQTT Discovery payload builder for the Tractive bridge.

Pure factory functions. Returns the list of `(component, unique_id_suffix,
payload_dict)` tuples that `main.py` then iterates and publishes via
`ThreadedPublisher.publish_discovery`. No MQTT, no I/O — same separation
as the parsers layer.

Entity organisation:
  - Phase 1 (live since 2026-05-22): 19 entities under the per-pet device
    `Tractive Bridge: <pet>` for biometric statuses, tracker metadata,
    position metadata.
  - Phase 2 (added 2026-05-26): ~30 more entities — numeric activity,
    vitality (RHR/RR in bpm), weekly report, bark/scratch counts,
    health-monitor statuses, alerts history, separation, account-scoped
    diagnostic info, subscription counts. Account-scoped entities live
    under a second device `Tractive Account` so the per-pet view stays
    focused on the pet.
"""

from __future__ import annotations

from ha_mqtt_bridge import (
    availability_block,
    build_device_block,
    build_discovery_payload,
)

from parsers import Pet


# Six monitor types observed in /health-alerts/status. Documented here so
# discovery + publish helpers can iterate the same source-of-truth list.
HEALTH_MONITOR_TYPES = [
    "ACTIVITY_DEGRADATION",
    "LONG_TERM_ACTIVITY_DEGRADATION",
    "RESTING_HEART_RATE",
    "RESTING_RESPIRATORY_RATE",
    "SEPARATION_ANXIETY",
    "SLEEP_INTERRUPTIONS",
]

# Event-timeline event types. Each gets a `last_<type>_at` timestamp
# sensor + (where applicable) `last_<type>_zone` string sensor in
# discovery. Period events also get `last_<type>_duration_seconds`.
TIMELINE_EVENT_TYPES = [
    "walk",
    "dailyActivityGoalReached",
    "safezoneEnter",
    "safezoneExit",
    "dangerzoneEnter",
    "dangerzoneExit",
    "insidePowerSavingZone",
]
PERIOD_EVENT_TYPES = {"walk", "insidePowerSavingZone"}
ZONE_EVENT_TYPES = {"safezoneEnter", "safezoneExit",
                    "dangerzoneEnter", "dangerzoneExit",
                    "insidePowerSavingZone"}


# `build_discovery_payload` in the shared toolkit accepts only a curated
# subset of HA Discovery fields. `device_tracker` needs `source_type` +
# `payload_home` / `payload_not_home`, which aren't in that curated set —
# so we hand-build the payload here rather than expanding the toolkit for
# one consumer. The shape mirrors what `build_discovery_payload` produces.
def _device_tracker_payload(
    *, name: str, unique_id: str, state_topic: str, json_attributes_topic: str,
    device: dict, avail: dict, icon: str,
) -> dict:
    payload = {
        "name": name,
        "unique_id": unique_id,
        "object_id": unique_id,
        "state_topic": state_topic,
        "json_attributes_topic": json_attributes_topic,
        "payload_home": "home",
        "payload_not_home": "not_home",
        "source_type": "gps",
        "icon": icon,
        "device": device,
        **avail,
    }
    return payload


def _pet_device_block(pet: Pet) -> dict:
    return build_device_block(
        identifiers=[f"tractive_mqtt_bridge_{pet.pet_id}"],
        name=f"Tractive Bridge: {pet.name}",
        manufacturer="Tractive",
        model="TG6A",
    )


def _account_device_block() -> dict:
    return build_device_block(
        identifiers=["tractive_mqtt_bridge_account"],
        name="Tractive Account",
        manufacturer="Tractive",
        model="Cloud account",
    )


# Phase 2 per-pet entity definitions. Format: (component, slug, name, extras).
# `extras` are splatted into build_discovery_payload as keyword args; common
# fields like state_topic + unique_id + device are added by the loop below.
PHASE2_PET_ENTITIES: list[tuple[str, str, str, dict]] = [
    # ---- daily activity ----
    ("sensor", "activity_minutes_today", "Activity Minutes Today",
     {"unit_of_measurement": "min", "state_class": "measurement", "icon": "mdi:run"}),
    ("sensor", "activity_minutes_goal_today", "Activity Goal Today",
     {"unit_of_measurement": "min", "state_class": "measurement",
      "icon": "mdi:target", "entity_category": "diagnostic"}),
    ("sensor", "activity_goal_pct_today", "Activity Goal % Today",
     {"unit_of_measurement": "%", "state_class": "measurement", "icon": "mdi:percent-circle"}),
    ("sensor", "activity_calories_today", "Calories Burned Today",
     {"unit_of_measurement": "kcal", "state_class": "measurement", "icon": "mdi:fire"}),
    ("sensor", "activity_streak_days", "Activity Streak",
     {"unit_of_measurement": "d", "state_class": "measurement", "icon": "mdi:fire-circle"}),
    ("sensor", "activity_breed_avg_minutes", "Breed Average Activity",
     {"unit_of_measurement": "min", "state_class": "measurement",
      "icon": "mdi:dog", "entity_category": "diagnostic"}),
    ("sensor", "activity_breed_deviation_pct", "Breed Activity Deviation",
     {"unit_of_measurement": "%", "state_class": "measurement",
      "icon": "mdi:trending-up", "entity_category": "diagnostic"}),
    ("sensor", "activity_age_group", "Age Group",
     {"icon": "mdi:cake-variant", "entity_category": "diagnostic"}),

    # ---- weekly report (numeric vitals — what HA's integration leaves on the floor) ----
    ("sensor", "activity_avg_minutes_7d", "Activity Avg 7d",
     {"unit_of_measurement": "min", "state_class": "measurement", "icon": "mdi:chart-line"}),
    ("sensor", "calories_avg_7d", "Calories Avg 7d",
     {"unit_of_measurement": "kcal", "state_class": "measurement", "icon": "mdi:fire"}),
    ("sensor", "sleep_avg_minutes_7d", "Sleep Avg 7d",
     {"unit_of_measurement": "min", "state_class": "measurement", "icon": "mdi:sleep"}),
    ("sensor", "sleep_interruptions_avg_7d", "Sleep Interruptions Avg 7d",
     {"state_class": "measurement", "icon": "mdi:sleep-off"}),
    ("sensor", "resting_heart_rate_bpm", "Resting Heart Rate",
     {"unit_of_measurement": "bpm", "state_class": "measurement", "icon": "mdi:heart-pulse"}),
    ("sensor", "resting_respiratory_rate_brpm", "Resting Respiratory Rate",
     {"unit_of_measurement": "br/min", "state_class": "measurement", "icon": "mdi:lungs"}),
    ("sensor", "weekly_summary_text", "Weekly Summary",
     {"icon": "mdi:text-box-outline"}),
    ("sensor", "weekly_period_from", "Weekly Period From",
     {"icon": "mdi:calendar-start", "entity_category": "diagnostic"}),
    ("sensor", "weekly_period_to", "Weekly Period To",
     {"icon": "mdi:calendar-end", "entity_category": "diagnostic"}),

    # ---- bark / scratch numerics ----
    ("sensor", "bark_count_today", "Bark Count Today",
     {"state_class": "measurement", "icon": "mdi:dog-side"}),
    ("sensor", "bark_count_upper_bound", "Bark Upper Bound",
     {"state_class": "measurement", "icon": "mdi:chart-bell-curve",
      "entity_category": "diagnostic"}),
    ("sensor", "scratch_seconds_today", "Scratch Seconds Today",
     {"unit_of_measurement": "s", "state_class": "measurement", "icon": "mdi:paw"}),
    ("sensor", "scratch_events_today", "Scratch Events Today",
     {"state_class": "measurement", "icon": "mdi:paw"}),
    ("sensor", "scratch_seconds_upper_bound", "Scratch Upper Bound",
     {"unit_of_measurement": "s", "state_class": "measurement",
      "icon": "mdi:chart-bell-curve", "entity_category": "diagnostic"}),

    # ---- health alerts history ----
    ("sensor", "health_alerts_history_count", "Health Alerts History",
     {"state_class": "measurement", "icon": "mdi:history",
      "entity_category": "diagnostic"}),
    ("sensor", "last_health_alert_type", "Last Health Alert Type",
     {"icon": "mdi:bell-alert"}),
    ("sensor", "last_health_alert_detected_at", "Last Health Alert Detected",
     {"device_class": "timestamp", "icon": "mdi:bell-alert-outline"}),
    ("sensor", "last_health_alert_seen_at", "Last Health Alert Seen",
     {"device_class": "timestamp", "icon": "mdi:eye-check",
      "entity_category": "diagnostic"}),

    # ---- separation phases ----
    ("binary_sensor", "separation_ongoing", "Separation Ongoing",
     {"device_class": "presence", "payload_on": "ON", "payload_off": "OFF",
      "icon": "mdi:home-export-outline"}),
    ("sensor", "separation_ongoing_count", "Separation Phases Ongoing",
     {"state_class": "measurement", "icon": "mdi:counter",
      "entity_category": "diagnostic"}),
]


def _phase2_monitor_entities() -> list[tuple[str, str, str, dict]]:
    """One sensor + one problem-binary per health monitor type."""
    out: list[tuple[str, str, str, dict]] = []
    for mtype in HEALTH_MONITOR_TYPES:
        slug = f"monitor_{mtype.lower()}"
        pretty = mtype.replace("_", " ").title()
        out.append((
            "sensor", f"{slug}_status", f"{pretty} Monitor",
            {"icon": "mdi:monitor-eye", "entity_category": "diagnostic"},
        ))
        out.append((
            "binary_sensor", f"{slug}_alert", f"{pretty} Alert",
            {"device_class": "problem", "payload_on": "ON", "payload_off": "OFF",
             "icon": "mdi:alert"},
        ))
    return out


def _timeline_event_entities() -> list[tuple[str, str, str, dict]]:
    """Last-event-of-type retained-state sensors per event-timeline type.

    Each type gets a `last_<type>_at` timestamp; zone types add a
    `last_<type>_zone` string; period types add a `last_<type>_duration_s`
    int. Plus `binary_sensor.inside_safezone` and `inside_dangerzone`
    derived from the most recent zone enter/exit pair.
    """
    icons = {
        "walk": "mdi:walk",
        "dailyActivityGoalReached": "mdi:trophy",
        "safezoneEnter": "mdi:home-circle",
        "safezoneExit": "mdi:home-export-outline",
        "dangerzoneEnter": "mdi:alert-circle",
        "dangerzoneExit": "mdi:alert-circle-outline",
        "insidePowerSavingZone": "mdi:wifi-marker",
    }
    out: list[tuple[str, str, str, dict]] = []
    for etype in TIMELINE_EVENT_TYPES:
        snake = _camel_to_snake(etype)
        pretty = _camel_to_pretty(etype)
        icon = icons.get(etype, "mdi:bell-outline")
        out.append((
            "sensor", f"last_{snake}_at", f"Last {pretty}",
            {"device_class": "timestamp", "icon": icon},
        ))
        if etype in ZONE_EVENT_TYPES:
            out.append((
                "sensor", f"last_{snake}_zone", f"Last {pretty} Zone",
                {"icon": "mdi:map-marker-radius", "entity_category": "diagnostic"},
            ))
        if etype in PERIOD_EVENT_TYPES:
            out.append((
                "sensor", f"last_{snake}_duration_s", f"Last {pretty} Duration",
                {"unit_of_measurement": "s", "state_class": "measurement",
                 "icon": "mdi:timer-outline"},
            ))
    # Derived "currently inside" binaries — flipped from the most recent
    # enter/exit pair the bridge has observed.
    out.append((
        "binary_sensor", "inside_safezone", "Inside Safezone",
        {"device_class": "presence", "payload_on": "ON", "payload_off": "OFF",
         "icon": "mdi:home-circle"},
    ))
    out.append((
        "binary_sensor", "inside_dangerzone", "Inside Dangerzone",
        {"device_class": "problem", "payload_on": "ON", "payload_off": "OFF",
         "icon": "mdi:alert-circle"},
    ))
    return out


def _camel_to_snake(s: str) -> str:
    out = []
    for i, c in enumerate(s):
        if c.isupper() and i > 0:
            out.append("_")
        out.append(c.lower())
    return "".join(out)


def _camel_to_pretty(s: str) -> str:
    # camelCase → "Camel Case"
    parts: list[str] = []
    buf: list[str] = []
    for c in s:
        if c.isupper() and buf:
            parts.append("".join(buf))
            buf = [c]
        else:
            buf.append(c)
    if buf:
        parts.append("".join(buf))
    return " ".join(p.capitalize() for p in parts)


PHASE2_ACCOUNT_ENTITIES: list[tuple[str, str, str, dict]] = [
    ("sensor", "account_email", "Account Email",
     {"icon": "mdi:email", "entity_category": "diagnostic"}),
    ("sensor", "account_first_name", "Account First Name",
     {"icon": "mdi:account", "entity_category": "diagnostic"}),
    ("sensor", "account_country", "Account Country",
     {"icon": "mdi:flag", "entity_category": "diagnostic"}),
    ("sensor", "account_locale", "Account Locale",
     {"icon": "mdi:translate", "entity_category": "diagnostic"}),
    ("sensor", "account_distance_unit", "Distance Unit",
     {"icon": "mdi:ruler", "entity_category": "diagnostic"}),
    ("sensor", "account_weight_unit", "Weight Unit",
     {"icon": "mdi:weight", "entity_category": "diagnostic"}),
    ("sensor", "account_activated_at", "Account Activated",
     {"device_class": "timestamp", "icon": "mdi:calendar-check",
      "entity_category": "diagnostic"}),
    ("sensor", "notifications_count", "Notifications",
     {"state_class": "measurement", "icon": "mdi:bell"}),
    ("sensor", "shares_count", "Shared With",
     {"state_class": "measurement", "icon": "mdi:share-variant",
      "entity_category": "diagnostic"}),
    ("sensor", "subscriptions_count", "Subscriptions",
     {"state_class": "measurement", "icon": "mdi:credit-card-check",
      "entity_category": "diagnostic"}),
    ("binary_sensor", "subscription_active", "Subscription Active",
     {"device_class": "connectivity", "payload_on": "ON", "payload_off": "OFF",
      "icon": "mdi:credit-card-check", "entity_category": "diagnostic"}),
]


def discovery_specs(
    pet: Pet, topic_prefix: str, lwt_topic: str
) -> list[tuple[str, str, dict]]:
    """Back-compat single-pet entry point — returns per-pet + account
    entities in one list. Multi-pet callers should use
    `discovery_specs_pet` + `discovery_specs_account` separately so the
    account-scoped specs only fire once per bridge instance."""
    return discovery_specs_pet(pet, topic_prefix, lwt_topic) + \
           discovery_specs_account(topic_prefix, lwt_topic)


def discovery_specs_pet(
    pet: Pet, topic_prefix: str, lwt_topic: str
) -> list[tuple[str, str, dict]]:
    """Per-pet HA Discovery payloads.

    Returns a list of `(component, unique_id_suffix, payload)` tuples
    suitable for splatting into `pub.publish_discovery(component=...,
    unique_id=..., payload=...)`. The `unique_id_suffix` is the slash-joined
    `<device_unique_id>/<slug>` and IS NOT the HA `unique_id` field on the
    payload — it's the path component the publisher uses to build the
    discovery topic.
    """
    items: list[tuple[str, str, dict]] = []
    avail = availability_block(lwt_topic)

    # ---------------------- per-pet device ----------------------
    pet_dev_uid = f"tractive_mqtt_bridge_{pet.pet_id}"
    pet_device = _pet_device_block(pet)
    pet_base = f"{topic_prefix}/{pet.pet_id}"

    phase1_entities: list[tuple[str, str, str, dict]] = [
        # ---- biometric raw status sensors ----
        ("sensor", "bark_status", "Bark Status",
         {"icon": "mdi:dog-side", "entity_category": "diagnostic"}),
        ("sensor", "heart_rate_status", "Resting Heart Rate Status",
         {"icon": "mdi:heart-pulse", "entity_category": "diagnostic"}),
        ("sensor", "respiratory_rate_status", "Resting Respiratory Rate Status",
         {"icon": "mdi:lungs", "entity_category": "diagnostic"}),
        ("sensor", "scratch_status", "Scratch Status",
         {"icon": "mdi:paw", "entity_category": "diagnostic"}),
        # ---- biometric alert binaries ----
        ("binary_sensor", "bark_alert", "Bark Alert",
         {"device_class": "problem", "payload_on": "ON", "payload_off": "OFF", "icon": "mdi:dog-side"}),
        ("binary_sensor", "heart_rate_alert", "Heart Rate Alert",
         {"device_class": "problem", "payload_on": "ON", "payload_off": "OFF", "icon": "mdi:heart-pulse"}),
        ("binary_sensor", "respiratory_rate_alert", "Respiratory Rate Alert",
         {"device_class": "problem", "payload_on": "ON", "payload_off": "OFF", "icon": "mdi:lungs"}),
        ("binary_sensor", "scratch_alert", "Scratch Alert",
         {"device_class": "problem", "payload_on": "ON", "payload_off": "OFF", "icon": "mdi:paw"}),
        # ---- health alerts counter ----
        ("sensor", "unseen_health_alerts", "Unseen Health Alerts",
         {"icon": "mdi:bell-alert", "state_class": "measurement"}),
        ("sensor", "health_data_synced_at", "Health Data Synced",
         {"device_class": "timestamp", "icon": "mdi:cloud-sync", "entity_category": "diagnostic"}),
        # ---- tracker state metadata ----
        ("sensor", "tracker_state", "Tracker State",
         {"icon": "mdi:cog", "entity_category": "diagnostic"}),
        ("sensor", "tracker_state_reason", "Tracker State Reason",
         {"icon": "mdi:information", "entity_category": "diagnostic"}),
        ("sensor", "tracker_battery_state", "Tracker Battery State",
         {"icon": "mdi:battery-alert", "entity_category": "diagnostic"}),
        ("sensor", "tracker_charging_state", "Tracker Charging State",
         {"icon": "mdi:battery-charging", "entity_category": "diagnostic"}),
        ("sensor", "tracker_firmware", "Tracker Firmware",
         {"icon": "mdi:chip", "entity_category": "diagnostic"}),
        ("sensor", "tracker_hw_edition", "Tracker Hardware Edition",
         {"icon": "mdi:tag", "entity_category": "diagnostic"}),
        # ---- position metadata ----
        ("sensor", "pos_sensor_used", "Position Sensor Used",
         {"icon": "mdi:crosshairs-gps", "entity_category": "diagnostic"}),
        ("sensor", "pos_uncertainty", "Position Uncertainty",
         {"unit_of_measurement": "m", "state_class": "measurement", "icon": "mdi:radius-outline",
          "entity_category": "diagnostic"}),
        ("sensor", "pos_accuracy_quality", "Position Accuracy",
         {"icon": "mdi:crosshairs", "entity_category": "diagnostic"}),
        # NOTE: `clip_mounted_state` and `temperature_state` from device_hw_report
        # are perpetually null on the TG6A "BLACK-LINES" model — the hardware
        # doesn't have a clip-mount-detect sensor or an internal thermometer,
        # so the cloud never populates them. We omit them from discovery to
        # avoid two permanently-Unknown HA entities. If a future Tractive
        # model that supports them gets paired to this account, add capability
        # detection from `tracker.capabilities` before publishing.
    ]
    for component, slug, name, extras in (
        phase1_entities + PHASE2_PET_ENTITIES + _phase2_monitor_entities()
        + _timeline_event_entities()
    ):
        uid = f"{pet_dev_uid}_{slug}"
        items.append((
            component,
            f"{pet_dev_uid}/{slug}",
            build_discovery_payload(
                name=name, unique_id=uid, object_id=uid,
                state_topic=f"{pet_base}/{slug}", device=pet_device, **avail, **extras,
            ),
        ))

    # ---------------------- Phase 3: position + device_tracker ----------------------
    # Four numeric sensors for InfluxDB ingest + a device_tracker entity
    # that places the pet on HA's Lovelace map. `device_tracker` state is
    # computed `home`/`not_home` from `sensor_used`: the Tractive cloud
    # reports KNOWN_WIFI when the tracker is inside its configured
    # power-saving Wi-Fi zone, which this integration treats as home.
    for component, slug, name, extras in [
        ("sensor", "latitude", "Latitude",
         {"unit_of_measurement": "°", "state_class": "measurement",
          "icon": "mdi:latitude", "entity_category": "diagnostic"}),
        ("sensor", "longitude", "Longitude",
         {"unit_of_measurement": "°", "state_class": "measurement",
          "icon": "mdi:longitude", "entity_category": "diagnostic"}),
        ("sensor", "speed_ms", "Speed",
         {"unit_of_measurement": "m/s", "state_class": "measurement",
          "device_class": "speed", "icon": "mdi:speedometer"}),
        ("sensor", "altitude_m", "Altitude",
         {"unit_of_measurement": "m", "state_class": "measurement",
          "device_class": "distance", "icon": "mdi:elevation-rise"}),
        ("sensor", "position_fix_at", "Position Fix Time",
         {"device_class": "timestamp", "icon": "mdi:clock-outline",
          "entity_category": "diagnostic"}),
    ]:
        uid = f"{pet_dev_uid}_{slug}"
        items.append((
            component, f"{pet_dev_uid}/{slug}",
            build_discovery_payload(
                name=name, unique_id=uid, object_id=uid,
                state_topic=f"{pet_base}/{slug}", device=pet_device, **avail, **extras,
            ),
        ))

    # device_tracker (hand-built since the toolkit doesn't accept source_type)
    dt_uid = f"{pet_dev_uid}_device_tracker"
    items.append((
        "device_tracker", f"{pet_dev_uid}/device_tracker",
        _device_tracker_payload(
            name="Location", unique_id=dt_uid,
            state_topic=f"{pet_base}/device_tracker/state",
            json_attributes_topic=f"{pet_base}/device_tracker/attrs",
            device=pet_device, avail=avail, icon="mdi:dog",
        ),
    ))

    return items


def discovery_specs_account(
    topic_prefix: str, lwt_topic: str,
) -> list[tuple[str, str, dict]]:
    """Account-scoped HA Discovery payloads. Call once per bridge instance
    regardless of pet count."""
    items: list[tuple[str, str, dict]] = []
    avail = availability_block(lwt_topic)
    acct_dev_uid = "tractive_mqtt_bridge_account"
    acct_device = _account_device_block()
    acct_base = f"{topic_prefix}/account"
    for component, slug, name, extras in PHASE2_ACCOUNT_ENTITIES:
        uid = f"{acct_dev_uid}_{slug}"
        items.append((
            component,
            f"{acct_dev_uid}/{slug}",
            build_discovery_payload(
                name=name, unique_id=uid, object_id=uid,
                state_topic=f"{acct_base}/{slug}", device=acct_device, **avail, **extras,
            ),
        ))
    return items
