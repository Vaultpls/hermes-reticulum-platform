#!/usr/bin/env python3
"""End-to-end LXST telephone test: one process calls another and hangs up.

Server role boots a TelephoneBridge (the plugin's headless endpoint) on a
loopback TCP interface and records whatever the caller transmits. Client role
boots a bare LXST Telephone with file-backed audio objects, dials the server,
waits for the call to be established, transmits a 440 Hz tone for a few
seconds, then hangs up. The orchestrator checks that both sides produced audio
and that the server's transcript callback fired.

    python scripts/phone_selftest.py            # run the whole test
    python scripts/phone_selftest.py server DIR # internal roles
    python scripts/phone_selftest.py client DIR
"""

from __future__ import annotations

# pyright: reportMissingImports=false
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time

PORT = 45980
TONE_SECONDS = 6
CALL_SECONDS = 4
OUTGOING = os.environ.get("PHONE_SELFTEST_OUTGOING") == "1"


def _wait_for(path: str, deadline: float) -> bool:
    while time.time() < deadline:
        if os.path.isfile(path):
            return True
        time.sleep(0.2)
    return False


def _write(path: str, data: dict) -> None:
    with open(path, "w") as handle:
        json.dump(data, handle, indent=2)


def _tone(path: str, frequency: int = 440, seconds: float = TONE_SECONDS) -> None:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        raise SystemExit("ffmpeg is required for the phone selftest")
    subprocess.run(
        [ffmpeg, "-v", "error", "-y", "-f", "lavfi",
         "-i", f"sine=frequency={frequency}:duration={seconds}:sample_rate=48000",
         "-ac", "1", "-c:a", "libopus", "-b:a", "24k", "-application", "voip",
         "-f", "ogg", path],
        check=True, stdin=subprocess.DEVNULL)


def run_server(workdir: str) -> int:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("telephony").setLevel(logging.DEBUG)
    logging.getLogger("call_audio").setLevel(logging.DEBUG)
    import RNS
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from telephony import TelephoneBridge

    cfg = os.path.join(workdir, "server-rns")
    os.makedirs(cfg, exist_ok=True)
    with open(os.path.join(cfg, "config"), "w") as handle:
        handle.write(
            "[reticulum]\n  enable_transport = No\n  share_instance = No\n"
            "  shared_instance_port = 45981\n  instance_control_port = 45982\n"
            "[interfaces]\n  [[loop]]\n    type = TCPServerInterface\n"
            "    enabled = yes\n    listen_ip = 127.0.0.1\n"
            f"    listen_port = {PORT}\n")
    RNS.Reticulum(configdir=cfg, loglevel=RNS.LOG_WARNING)

    identity_path = os.path.join(workdir, "server-identity")
    identity = RNS.Identity()
    identity.to_file(identity_path)
    expected_hash = RNS.Destination.hash(identity, "lxst", "telephony")

    result_path = os.path.join(workdir, "server-result.json")
    utterance_path = os.path.join(workdir, "server-utterance.json")
    _write(os.path.join(workdir, "server-ready.json"),
           {"address": expected_hash.hex(), "identity": identity_path,
            "identity_hash": identity.hash.hex()})

    # Let the caller connect before the first announce: RNS rate-limits a
    # destination to roughly one announce per 72 seconds, so an announce sent
    # before the link exists would leave the caller waiting a minute.
    time.sleep(6)

    def on_utterance(call):
        _write(utterance_path, {
            "caller": call.get("caller"),
            "index": call.get("utterance_index"),
            "duration": call.get("utterance_duration"),
            "transcript": call.get("transcript"),
            "path": call.get("utterance_path"),
            "size": (os.path.getsize(call["utterance_path"])
                     if call.get("utterance_path") and os.path.isfile(call["utterance_path"]) else 0),
        })

    def on_transcript(call):
        _write(result_path, {
            "caller": call.get("caller"),
            "path": call.get("path"),
            "size": os.path.getsize(call["path"]) if call.get("path") and os.path.isfile(call["path"]) else 0,
            "duration": call.get("duration"),
            "error": call.get("error"),
            "transcript": call.get("transcript"),
            "live": bool(call.get("live")),
            "utterances": int(call.get("utterances") or 0),
        })

    bridge = TelephoneBridge(
        identity,
        storage_path=os.path.join(workdir, "server-storage"),
        allowed=lambda caller: True,
        on_transcript=on_transcript,
        on_utterance=on_utterance,
        max_call_seconds=30,
        auto_answer=0.5,
    )
    if not bridge.start():
        _write(result_path, {"error": "bridge failed to start"})
        return 1
    # Skip the real STT model in the test: the point is capture, segmentation,
    # and the callback. The live worker still exercises encoding and dispatch.
    bridge._transcribe = lambda path: {
        "success": True,
        "transcript": "selftest audible tone",
    }
    print(f"server telephone {bridge.address}", flush=True)

    if OUTGOING:
        client_ready = os.path.join(workdir, "client-ready.json")
        if not _wait_for(client_ready, time.time() + 30):
            _write(result_path, {"error": "outgoing peer never became ready"})
            return 1
        with open(client_ready) as handle:
            client_info = json.load(handle)
        peer = RNS.Identity.from_file(client_info["identity"])
        dial_result = bridge.dial(
            peer, reply_to=RNS.Destination.hash(peer, "lxmf", "delivery").hex())
        _write(os.path.join(workdir, "dial-result.json"), dial_result)
        if not dial_result.get("success"):
            _write(result_path, {"error": str(dial_result)})
            return 1

    reply_tone = os.path.join(workdir, "reply-tone.ogg")
    _tone(reply_tone, frequency=880, seconds=2.0)

    def speak_into_call():
        deadline = time.time() + 30
        while time.time() < deadline:
            call = bridge.active_call()
            if call:
                time.sleep(1.0)  # after answer, while the caller is connected
                if bridge.speak(call["reply_to"], audio_path=reply_tone):
                    print("server queued an in-call reply", flush=True)
                return
            time.sleep(0.1)

    threading.Thread(target=speak_into_call, name="selftest-reply", daemon=True).start()

    deadline = time.time() + 90
    while time.time() < deadline:
        if os.path.isfile(result_path):
            # Give the live worker a moment to finish the final utterance.
            time.sleep(2)
            return 0
        time.sleep(1)
    _write(result_path, {"error": "timeout waiting for a call"})
    return 1


def run_client(workdir: str) -> int:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("telephony").setLevel(logging.DEBUG)
    logging.getLogger("call_audio").setLevel(logging.DEBUG)
    import RNS
    from LXST.Primitives import Telephony as telephony
    from LXST.Codecs import Raw
    from LXST.Sinks import OpusFileSink
    from LXST.Sources import OpusFileSource
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from telephony import make_call_recorder

    ready = os.path.join(workdir, "server-ready.json")
    if not _wait_for(ready, time.time() + 60):
        _write(os.path.join(workdir, "client-result.json"), {"error": "server never became ready"})
        return 1
    with open(ready) as handle:
        info = json.load(handle)

    cfg = os.path.join(workdir, "client-rns")
    os.makedirs(cfg, exist_ok=True)
    with open(os.path.join(cfg, "config"), "w") as handle:
        handle.write(
            "[reticulum]\n  enable_transport = No\n  share_instance = No\n"
            "  shared_instance_port = 45983\n  instance_control_port = 45984\n"
            "[interfaces]\n  [[loop]]\n    type = TCPClientInterface\n"
            "    enabled = yes\n    target_host = 127.0.0.1\n"
            f"    target_port = {PORT}\n")
    RNS.Reticulum(configdir=cfg, loglevel=RNS.LOG_WARNING)

    identity = RNS.Identity()
    out_path = os.path.join(workdir, "client-heard.ogg")
    tone = os.path.join(workdir, "tone.ogg")
    client_sinks = []

    def _client_sink(preferred_device=None, low_latency=False, **kw):
        # LXST recreates the line sink during call setup. Give each instance its
        # own file so the final recorder cannot race an earlier one's ffmpeg.
        path = f"{out_path}.{len(client_sinks) + 1}.ogg"
        recorder = make_call_recorder(path)
        client_sinks.append(recorder)
        return recorder

    from LXST.Generators import ToneSource
    telephony.LineSink = _client_sink
    telephony.LineSource = lambda preferred_device=None, target_frame_ms=None, codec=None, sink=None, **kw: ToneSource(
        frequency=440, gain=0.3, ease=False, target_frame_ms=target_frame_ms or 60,
        codec=codec, sink=sink)

    telephone = telephony.Telephone(identity)
    established = threading.Event()
    telephone.set_established_callback(lambda remote: established.set())

    if OUTGOING:
        telephone.auto_answer = 1.5
        def _rang(remote):
            print("client handset ringing", flush=True)
            _write(os.path.join(workdir, "client-ring.json"), {"rang": True})
        telephone.set_ringing_callback(_rang)
        telephone.announce()
        identity_path = os.path.join(workdir, "client-identity")
        identity.to_file(identity_path)
        _write(os.path.join(workdir, "client-ready.json"), {"identity": identity_path})

    dest_hash = bytes.fromhex(info["address"])
    deadline = time.time() + 90
    remote_identity = None
    while time.time() < deadline:
        remote_identity = RNS.Identity.recall(dest_hash)
        if remote_identity is not None:
            break
        time.sleep(1)
    if remote_identity is None:
        _write(os.path.join(workdir, "client-result.json"), {"error": "never heard the announce"})
        return 1

    if not OUTGOING:
        telephone.call(remote_identity)
    if not established.wait(45):
        _write(os.path.join(workdir, "client-result.json"), {"error": "call never established"})
        return 1
    # The patched microphone source transmits a 440 Hz tone for the whole
    # call, which gives the receiving VAD a deterministic speech-like signal.
    sent = {"n": 0}
    packetizer = getattr(getattr(telephone, "active_call", None), "packetizer", None)
    if packetizer is not None:
        original = packetizer.handle_frame
        def _count(frame, source=None, _orig=original):
            sent["n"] += 1
            return _orig(frame, source)
        packetizer.handle_frame = _count
    time.sleep(0.8)
    diag = {
        "packetizer_sent": sent["n"],
        "packetizer_run": bool(getattr(packetizer, "should_run", False)),
        "packetizer_squelched": bool(getattr(packetizer, "squelched", True)),
        "dest_type": type(getattr(packetizer, "destination", None)).__name__,
        "audio_input": type(getattr(telephone, "audio_input", None)).__name__,
        "input_running": bool(getattr(getattr(telephone, "audio_input", None), "should_run", False)),
        "transmit_mixer_running": bool(getattr(getattr(telephone, "transmit_mixer", None), "should_run", False)),
        "transmit_pipeline": type(getattr(telephone, "transmit_pipeline", None)).__name__,
        "packetizer": type(getattr(telephone, "packetizer", None)).__name__,
        "call_status": getattr(telephone, "call_status", None),
        "mixer_sources": len(getattr(getattr(telephone, "transmit_mixer", None), "incoming_frames", {}) or {}),
        "mixer_sink": type(getattr(getattr(telephone, "transmit_mixer", None), "_sink", None)).__name__,
        "mixer_codec": type(getattr(getattr(telephone, "transmit_mixer", None), "codec", None)).__name__,
        "mixer_frames": [len(v) for v in (getattr(getattr(telephone, "transmit_mixer", None), "incoming_frames", {}) or {}).values()],
    }
    print("CLIENT DIAG", diag, flush=True)
    # Read the negotiated profile while the call is still up: after hangup()
    # active_call is gone and the field is always null.
    profile_during_call = getattr(getattr(telephone, "active_call", None), "profile", None)
    print("CLIENT PROFILE", profile_during_call, flush=True)
    time.sleep(CALL_SECONDS - 1.0)
    telephone.hangup()
    # LXST does not always stop the current line sink when an outgoing call is
    # torn down in this harness; finalize every recorder explicitly.
    for sink in client_sinks:
        sink.stop()
    time.sleep(1)
    lived_path = client_sinks[-1].path if client_sinks else out_path
    _write(os.path.join(workdir, "client-result.json"), {
        "heard": os.path.getsize(lived_path) if os.path.isfile(lived_path) else 0,
        "path": lived_path,
        "profile": profile_during_call,
        "recorders": [{"frames": sink.frame_count, "path": sink.path}
                      for sink in client_sinks],
    })
    return 0


def _ffprobe(path: str) -> str:
    ffprobe = shutil.which("ffprobe")
    if not ffprobe or not os.path.isfile(path):
        return "missing"
    result = subprocess.run(
        [ffprobe, "-v", "error", "-show_entries", "stream=codec_name,duration",
         "-of", "default=nw=1", path], capture_output=True, text=True, timeout=30)
    return " ".join(result.stdout.split())


def orchestrate() -> int:
    workdir = tempfile.mkdtemp(prefix="phone-selftest-")
    _tone(os.path.join(workdir, "tone.ogg"))
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    script = os.path.abspath(__file__)
    env = dict(os.environ)
    env["PYTHONPATH"] = root + os.pathsep + env.get("PYTHONPATH", "")

    client = subprocess.Popen([sys.executable, script, "client", workdir], env=env)
    time.sleep(2)
    server = subprocess.Popen([sys.executable, script, "server", workdir], env=env)

    server_code = server.wait(timeout=180)
    client_code = client.wait(timeout=180)

    def load(name: str) -> dict:
        path = os.path.join(workdir, name)
        if not os.path.isfile(path):
            return {}
        with open(path) as handle:
            return json.load(handle)

    server_result = load("server-result.json")
    client_result = load("client-result.json")
    utterance_result = load("server-utterance.json")
    print("server:", json.dumps(server_result))
    print("client:", json.dumps(client_result))
    print("utterance:", json.dumps(utterance_result))

    checks = [
        ("server exited cleanly", server_code == 0),
        ("client exited cleanly", client_code == 0),
        ("server recorded audio", int(server_result.get("size") or 0) > 2000),
        ("server call lasted ~4s", 2.0 <= float(server_result.get("duration") or 0) <= 20.0),
        ("server transcript callback fired", "path" in server_result),
        ("live call mode was active", bool(server_result.get("live"))),
        ("live utterance callback fired", bool(utterance_result.get("transcript"))),
        ("live utterance audio was written", int(utterance_result.get("size") or 0) > 500),
        ("client heard the in-call reply", int(client_result.get("heard") or 0) > 4000),
    ]
    expected_profiles = {
        "ultra_low": 0x10, "very_low": 0x20, "low": 0x30, "medium": 0x40,
        "high": 0x50, "max": 0x60,
    }
    wanted = expected_profiles.get(os.environ.get("RETICULUM_CALL_PROFILE", "").strip().lower())
    if wanted is not None:
        checks.append((f"call negotiated profile 0x{wanted:02x}",
                       client_result.get("profile") == wanted))
    if OUTGOING:
        dial_result = load("dial-result.json")
        ring_result = load("client-ring.json")
        checks.append(("bridge initiated outgoing call", dial_result.get("status") == "dialing"))
        checks.append(("remote handset rang", bool(ring_result.get("rang"))))
    for label, ok in checks:
        print(("PASS " if ok else "FAIL ") + label)
    server_path = server_result.get("path")
    if server_path:
        print("recorded:", _ffprobe(server_path))
    client_path = client_result.get("path")
    if client_path:
        print("heard:", _ffprobe(client_path))
    ok = all(item[1] for item in checks)
    print("PHONE SELFTEST " + ("PASS" if ok else "FAIL") + f"  (workdir {workdir})")
    if ok and os.environ.get("PHONE_SELFTEST_KEEP") != "1":
        shutil.rmtree(workdir, ignore_errors=True)
    return 0 if ok else 1


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "server":
        raise SystemExit(run_server(sys.argv[2]))
    if len(sys.argv) >= 3 and sys.argv[1] == "client":
        raise SystemExit(run_client(sys.argv[2]))
    raise SystemExit(orchestrate())
