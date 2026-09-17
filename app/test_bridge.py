"""Unit tests for the Tractive bridge's parsers, derivations, HA
Discovery payloads, and the stale-channel reconnect logic — synthetic
fixtures only, no network access.

Mirrors govee-mqtt-bridge's `test_discovery_regression.py` layout: stub
out `requests`/`paho` (and point at the in-repo toolkit source) so the
bridge's own modules import cleanly without real credentials or the
vendored toolkit being pip-installed. Run with::

    cd app
    TRACTIVE_USERNAME=x TRACTIVE_PASSWORD=x TRACTIVE_CLIENT_ID=x \\
        TRACTIVE_USER_ID=x MQTT_PASSWORD=x python -m pytest test_bridge.py -q
"""

from __future__ import annotations

import os
import sys
import types
import unittest

_REQUIRED = {
    "TRACTIVE_USERNAME": "x@example.com",
    "TRACTIVE_PASSWORD": "test",
    "TRACTIVE_CLIENT_ID": "test-client",
    "TRACTIVE_USER_ID": "abcdef0123456789abcdef01",
    "MQTT_PASSWORD": "test",
}
for k, v in _REQUIRED.items():
    os.environ.setdefault(k, v)


def _stub_module(name: str, **attrs: object) -> None:
    if name in sys.modules:
        return
    mod = types.ModuleType(name)
    for attr_name, attr_val in attrs.items():
        setattr(mod, attr_name, attr_val)
    sys.modules[name] = mod


_stub_module("requests")
sys.modules["requests"].RequestException = Exception  # type: ignore[attr-defined]
sys.modules["requests"].get = lambda *a, **k: None  # type: ignore[attr-defined]
sys.modules["requests"].post = lambda *a, **k: None  # type: ignore[attr-defined]
sys.modules["requests"].request = lambda *a, **k: None  # type: ignore[attr-defined]
_stub_module("paho")
_stub_module("paho.mqtt")
mqtt_stub = types.ModuleType("paho.mqtt.client")


class _FakeCallbackAPIVersion:
    VERSION2 = 2


class _FakeMqttClient:
    def __init__(self, *_, **__):
        pass


mqtt_stub.CallbackAPIVersion = _FakeCallbackAPIVersion
mqtt_stub.Client = _FakeMqttClient
sys.modules["paho.mqtt.client"] = mqtt_stub

if "ha_mqtt_bridge" not in sys.modules:
    here = os.path.dirname(os.path.abspath(__file__))
    toolkit = os.path.normpath(
        os.path.join(here, "..", "..", "..", "_shared", "ha-mqtt-bridge-toolkit")
    )
    sys.path.insert(0, toolkit)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import discovery  # noqa: E402
import main  # noqa: E402
import parsers  # noqa: E402


# -------------------------------------------------------------- parsers / derivations


class TestDerivations(unittest.TestCase):
    def test_humanize_enum_title_case(self):
        self.assertEqual(parsers._humanize_enum("NOT_SYNCED_YET"), "Not Synced Yet")
        self.assertIsNone(parsers._humanize_enum(None))
        # A firmware-looking string (contains a period) passes through.
        self.assertEqual(parsers._humanize_enum("010.083-TRV6-014-ab8d9f"),
                          "010.083-TRV6-014-ab8d9f")

    def test_pos_accuracy_quality_buckets(self):
        self.assertEqual(parsers._pos_accuracy_quality(3), "Excellent")
        self.assertEqual(parsers._pos_accuracy_quality(10), "Good")
        self.assertEqual(parsers._pos_accuracy_quality(40), "Fair")
        self.assertEqual(parsers._pos_accuracy_quality(500), "Poor")
        self.assertIsNone(parsers._pos_accuracy_quality(None))
        self.assertIsNone(parsers._pos_accuracy_quality("not-a-number"))

    def test_obj_normalises_none_to_empty_dict(self):
        # A tracker that has never reported returns 200-with-no-body for
        # several endpoints; every dict parser must tolerate that (see
        # parsers._obj's own docstring for the outage this fixed).
        self.assertEqual(parsers._obj(None), {})
        self.assertEqual(parsers._obj({"a": 1}), {"a": 1})
        self.assertEqual(parsers._obj([1, 2]), {})


# -------------------------------------------------------------- discovery


class TestDiscoverySpecsPet(unittest.TestCase):
    def setUp(self):
        self.pet = parsers.Pet(
            pet_id="pet123", name="Fido", device_id="dev456", breed_ids=[],
        )

    def test_device_tracker_entity_present(self):
        items = discovery.discovery_specs_pet(self.pet, "tractive", "tractive/bridge/online")
        dt_entries = [i for i in items if i[0] == "device_tracker"]
        self.assertEqual(len(dt_entries), 1, "expected exactly one device_tracker entity")
        component, unique_id_suffix, payload = dt_entries[0]
        self.assertTrue(unique_id_suffix.endswith("/device_tracker"))
        self.assertEqual(payload["source_type"], "gps")
        self.assertEqual(payload["payload_home"], "home")
        self.assertEqual(payload["payload_not_home"], "not_home")
        self.assertEqual(
            payload["state_topic"], "tractive/pet123/device_tracker/state",
        )

    def test_no_slugify_leftover_reference(self):
        # `_slugify` was a dead no-op (`s.replace("_", "_")`) removed in
        # this pass — assert it's actually gone.
        self.assertFalse(hasattr(discovery, "_slugify"))


# -------------------------------------------------------------- stale-channel reconnect


class _FakeChannel:
    """Duck-typed stand-in for events.ChannelClient — only the surface
    `reconnect_channel_if_stale` touches (`.stale`, `.stop()`)."""

    def __init__(self, stale: bool, name: str = "channel"):
        self.stale = stale
        self.name = name
        self.stopped = False
        self.started = False

    def stop(self) -> None:
        self.stopped = True

    def start(self) -> None:
        self.started = True


class _NullLogger:
    def warning(self, *a, **k):
        pass


class TestReconnectChannelIfStale(unittest.TestCase):
    def test_none_channel_is_noop(self):
        result = main.reconnect_channel_if_stale(None, lambda: _FakeChannel(False), _NullLogger())
        self.assertIsNone(result)

    def test_fresh_channel_unchanged(self):
        fresh = _FakeChannel(stale=False, name="fresh")
        make_calls = []

        def make_channel():
            make_calls.append(1)
            return _FakeChannel(False, name="new")

        result = main.reconnect_channel_if_stale(fresh, make_channel, _NullLogger())
        self.assertIs(result, fresh)
        self.assertFalse(fresh.stopped)
        self.assertEqual(make_calls, [], "make_channel must not be called for a fresh channel")

    def test_stale_channel_is_replaced(self):
        stale = _FakeChannel(stale=True, name="stale")
        replacement = _FakeChannel(False, name="replacement")

        def make_channel():
            return replacement

        result = main.reconnect_channel_if_stale(stale, make_channel, _NullLogger())
        self.assertIs(result, replacement)
        self.assertTrue(stale.stopped, "the stale channel must be stopped")
        self.assertTrue(replacement.started, "the replacement channel must be started")


if __name__ == "__main__":
    unittest.main()


class TestChannelClientStale(unittest.TestCase):
    """`ChannelClient.stale` drives the main loop's reconnect: it must only
    report a connected channel that has gone silent."""

    def _client(self):
        import events
        return events.ChannelClient(lambda: {}, lambda _m: None, _NullLogger()), events

    def test_not_stale_before_first_connect(self):
        client, _ = self._client()
        self.assertFalse(client.stale)

    def test_not_stale_while_disconnected_and_backing_off(self):
        client, events = self._client()
        client._connected = False
        client._last_message_at = 0.0
        self.assertFalse(client.stale)

    def test_connected_and_recent_is_not_stale(self):
        import time
        client, _ = self._client()
        client._connected = True
        client._last_message_at = time.time()
        self.assertFalse(client.stale)

    def test_connected_and_silent_is_stale(self):
        import time
        client, events = self._client()
        client._connected = True
        client._last_message_at = time.time() - events.CHANNEL_STALE_AFTER - 1
        self.assertTrue(client.stale)
