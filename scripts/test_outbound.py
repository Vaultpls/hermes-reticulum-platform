#!/usr/bin/env python3
"""Offline tests for outbound Reticulum call routing and command matching."""

from __future__ import annotations

import json
import os
import sys
from types import SimpleNamespace
from unittest.mock import patch

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import RNS  # noqa: E402

import adapter as adapter_module  # noqa: E402
from adapter import ReticulumAdapter, _call_tool, _is_call_request  # noqa: E402
from telephony import TelephoneBridge  # noqa: E402


def test_matcher() -> None:
    positive = [
        "call me", "Call me.", "please call me back", "give me a call",
        "ring me", "phone me", "dial me", "give me a ring",
        "call me up", "can you call me?", "Could you give me a call now?",
        "would you ring me please",
    ]
    negative = [
        "call me maybe", "I will call you later", "don't call me",
        "call my wife", "call me at 5", "call me back tomorrow",
        "give me a callback", "the word call me", "",
    ]
    assert all(_is_call_request(text) for text in positive), "missed a call request"
    assert not any(_is_call_request(text) for text in negative), "false positive call request"


class FakeBridge:
    def __init__(self) -> None:
        self.busy: dict[str, str] | None = None
        self.dialed = []

    def active_call(self):
        return self.busy

    def dial(self, identity, *, reply_to, timeout=20.0):
        self.dialed.append((identity, reply_to))
        return {"success": True, "status": "dialing", "reply_to": reply_to}


def make_adapter(identity, allowed, bridge):
    obj = ReticulumAdapter.__new__(ReticulumAdapter)
    obj._telephone = bridge
    obj._is_allowed = lambda source_hash: source_hash in allowed
    obj._resolve_identity = lambda destination_hash: identity
    return obj


def test_call_peer() -> None:
    identity = RNS.Identity()
    target = RNS.Destination.hash(identity, "lxmf", "delivery").hex()
    bridge = FakeBridge()
    adapter = make_adapter(identity, {target}, bridge)

    bad = adapter.call_peer("not-a-destination")
    assert not bad["success"] and "LXMF" in bad["error"], bad

    denied_identity = RNS.Identity()
    denied_adapter = make_adapter(
        denied_identity, set(), FakeBridge())
    denied = denied_adapter.call_peer(
        RNS.Destination.hash(denied_identity, "lxmf", "delivery").hex())
    assert not denied["success"] and "authorized" in denied["error"], denied

    bridge.busy = {"caller": target}
    busy = adapter.call_peer(target)
    assert not busy["success"] and "busy" in busy["error"], busy
    bridge.busy = None

    unknown_adapter = make_adapter(None, {target}, FakeBridge())
    unknown = unknown_adapter.call_peer(target)
    assert not unknown["success"] and "unknown" in unknown["error"], unknown

    other = RNS.Identity()
    mismatch_adapter = make_adapter(other, {target}, FakeBridge())
    mismatch = mismatch_adapter.call_peer(target)
    assert not mismatch["success"] and "does not match" in mismatch["error"], mismatch

    result = adapter.call_peer(target)
    assert result["success"] and result["status"] == "dialing", result
    assert bridge.dialed and bridge.dialed[0][1] == target, bridge.dialed

    adapter_module._LIVE_ADAPTER = adapter
    tool_result = json.loads(_call_tool({"destination": target}))
    assert tool_result["success"] and tool_result["status"] == "dialing", tool_result
    adapter_module._LIVE_ADAPTER = None


def test_profile_cap_echo() -> None:
    from LXST.Primitives import Telephony

    profiles = Telephony.Profiles
    signalling = Telephony.Signalling

    class FakeTelephone:
        def __init__(self, incoming):
            self.active_call = SimpleNamespace(
                profile=profiles.QUALITY_MEDIUM, is_incoming=incoming,
                is_outgoing=not incoming)
            self.call_status = signalling.STATUS_ESTABLISHED
            self.signals = []

        def signal(self, value, call):
            assert call is self.active_call
            self.signals.append(value)

        def switch_profile(self, profile=None, from_signalling=False):
            assert profile is not None
            if self.active_call.profile == profile:
                return
            if self.call_status == signalling.STATUS_ESTABLISHED:
                self.active_call.profile = profile
                if not from_signalling:
                    self.signal(signalling.PREFERRED_PROFILE + profile, self.active_call)

    module = SimpleNamespace(Telephone=FakeTelephone, Profiles=profiles,
                             Signalling=signalling)
    bridge = TelephoneBridge.__new__(TelephoneBridge)
    with patch.dict(os.environ, {"RETICULUM_CALL_PROFILE": "medium"}):
        capped = bridge._telephone_class(module)

    for incoming in (False, True):
        phone = capped(incoming)
        # Above-cap proposals must be answered even if we're already at cap.
        for initial in (profiles.QUALITY_MEDIUM, profiles.BANDWIDTH_LOW):
            phone.active_call.profile = initial
            phone.signals.clear()
            phone.switch_profile(profiles.QUALITY_HIGH, from_signalling=True)
            assert phone.active_call.profile == profiles.QUALITY_MEDIUM
            assert phone.signals == [signalling.PREFERRED_PROFILE + profiles.QUALITY_MEDIUM]

        # An echoed cap terminates negotiation; lower proposals stay silent.
        phone.signals.clear()
        phone.switch_profile(profiles.QUALITY_MEDIUM, from_signalling=True)
        phone.switch_profile(profiles.BANDWIDTH_LOW, from_signalling=True)
        assert phone.active_call.profile == profiles.BANDWIDTH_LOW
        assert phone.signals == []

        # Local requests retain LXST's single normal announcement.
        phone.switch_profile(profiles.QUALITY_HIGH)
        assert phone.signals == [signalling.PREFERRED_PROFILE + profiles.QUALITY_MEDIUM]

        phone.call_status = signalling.STATUS_RINGING
        phone.signals.clear()
        phone.switch_profile(profiles.QUALITY_HIGH, from_signalling=True)
        assert phone.signals == []


if __name__ == "__main__":
    test_matcher()
    test_call_peer()
    test_profile_cap_echo()
    print("OUTBOUND UNIT TESTS PASS")
