# Architecture

A single Python process combining a background real-time connection with
three tiered poll loops.

```
channel.tractive.com/3/channel  ──long-poll──►  ChannelClient (daemon thread)
                                                        │
graph.tractive.com/4/           ──poll──►  tractive-mqtt-bridge  ──►  ThreadedPublisher  ──►  MQTT broker
aps-api.tractive.com/api/{1,2}/ ──poll──►        (FAST/MED/SLOW tiers +
event-timeline-api.tractive.com ──poll──►         per-pet EventStream)
```

## Auth

`POST /4/auth/token` on `graph.tractive.com` with a `platform_email` +
`platform_token` grant (Tractive's own naming for a plain username/password
login), using the public API client id. The returned token is valid for an
extended period (Tractive documents 60 days); the bridge treats an
authentication-looking failure from any endpoint as an expired session and
re-authenticates from scratch.

## Real-time push channel

`channel.tractive.com/3/channel` is a long-poll HTTP connection that streams
newline-delimited JSON position, hardware-info and tracker-status updates as
they happen. A daemon thread (`ChannelClient` in `events.py`) holds this
connection open and dispatches each event straight into the same publish
helpers the poll tiers use. If the channel goes quiet for longer than a
short timeout, the bridge falls back on the fast poll tier as a backstop
until the channel reconnects — so position updates keep flowing either way,
just less frequently.

## Poll tiers

| Tier | Default interval | What it fetches |
|---|---|---|
| Fast | 90 s | Tracker state, hardware report and position — this is the backstop the real-time channel covers most of the time |
| Medium | 300 s | Health overview (bark/heart-rate/respiratory-rate/scratch status), daily activity, bark/scratch day counts, health-alert monitor statuses, separation phases, notifications, and the incremental event-timeline poll |
| Slow | 3600 s | Weekly activity/health reports, account info, sharing list, subscription state |

## Event timeline

`prod.eu-central-1.event-timeline-api.tractive.com` (a regional host the
mobile app hardcodes rather than the unresolvable canonical hostname) serves
walk, safe-zone, danger-zone and power-saving-zone enter/exit, and daily
activity-goal-reached events. `EventStream` (in `events.py`) tracks which
event IDs have already been published (capped in memory) and, on the medium
tier, publishes any new one as both a one-shot JSON MQTT event (for a
time-series consumer) and a retained "last event of this type" state mirror
for Home Assistant. A startup backfill walks back `EVENT_BACKFILL_DAYS` of
history the same way.

## Module layout

| Path | Role |
|---|---|
| `app/main.py` | Login, the three poll-tier runners, publish helpers, Home Assistant Discovery wiring, main loop |
| `app/parsers.py` | Dataclasses and `parse_*` functions for every endpoint's response shape, plus small derivation helpers |
| `app/discovery.py` | Home Assistant Discovery payload factory, split into per-pet and per-account entity tables |
| `app/events.py` | `EventStream` (incremental event-timeline poller) and `ChannelClient` (the real-time push-channel consumer) |
| `app/requirements.txt` | `paho-mqtt`, `requests` |
| `_shared/ha-mqtt-bridge-toolkit/` | Vendored at publish time — MQTT client, Discovery payload builders, topic/timestamp helpers |
| `_shared/python-github-error-reporter/` | Vendored at publish time — optional production-error reporting |
