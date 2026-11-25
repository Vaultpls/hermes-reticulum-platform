#!/usr/bin/env python3
"""Self-test for the Reticulum/LXMF Hermes platform.

Two modes:

* ``unit`` — offline checks of the timestamp, ordering, Codec2 and voice
  helpers (no network, no RNS).
* default — two processes on loopback TCP:

  * ``server`` boots the real ``ReticulumAdapter`` against a temporary RNS
    config and LXMF storage, then answers every authorized inbound message.
  * ``client`` is a bare LXMF peer that announces itself, sends ``ping``,
    a voice note, and checks the reply text was timestamped and that the
    server's spoken reply arrived as an ``AM_OPUS_OGG`` voice note.

No public Reticulum transport nodes and no LoRa hardware are used. Run:

    python3 scripts/selftest.py

Requirements: the same Python environment Hermes uses (rns + lxmf installed)
and the Hermes source tree importable (set ``HERMES_SRC`` if it is not at
``~/.hermes/hermes-agent``). Exit code 0 means the full round trip worked.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_DIR = os.path.dirname(THIS_DIR)

AUDIO_MODE_OPUS_OGG = 0x10


def _fill_import_path() -> None:
    sys.path.insert(0, REPO_DIR)
    for candidate in (os.environ.get("HERMES_SRC"),
                      os.path.expanduser("~/.hermes/hermes-agent")):
        if candidate and os.path.isdir(os.path.join(candidate, "gateway")):
            sys.path.insert(0, candidate)
            break
    try:
        import gateway  # noqa: F401
    except ModuleNotFoundError:
        raise SystemExit(
            "Cannot import the Hermes gateway package. Set HERMES_SRC to the "
            "Hermes source directory (e.g. ~/.hermes/hermes-agent).")


def _rns_config(directory: str, *, role: str, port: int) -> None:
    os.makedirs(directory, exist_ok=True)
    if role == "server":
        interface = (
            "  [[Selftest TCP Server]]\n"
            "    type = TCPServerInterface\n"
            "    enabled = yes\n"
            "    listen_ip = 127.0.0.1\n"
            f"    listen_port = {port}\n"
            "    server_discovery = 0\n")
    else:
        interface = (
            "  [[Selftest TCP Client]]\n"
            "    type = TCPClientInterface\n"
            "    enabled = yes\n"
            "    target_host = 127.0.0.1\n"
            f"    target_port = {port}\n")
    with open(os.path.join(directory, "config"), "w", encoding="utf-8") as handle:
        handle.write(
            "[reticulum]\n"
            "  enable_transport = Yes\n"
            "  share_instance = No\n"
            f"  shared_instance_port = {port + 100}\n"
            f"  instance_control_port = {port + 200}\n\n"
            "[interfaces]\n" + interface)


def _make_opus(path: str, seconds: float = 1.0) -> bool:
    """Write a small Ogg/Opus tone; False when ffmpeg is unavailable."""
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        return False
    return subprocess.run(
        [ffmpeg, "-v", "error", "-y", "-f", "lavfi",
         "-i", f"sine=frequency=440:duration={seconds}:sample_rate=48000",
         "-ac", "1", "-c:a", "libopus", "-b:a", "24k", "-application", "voip",
         "-f", "ogg", path],
        capture_output=True, stdin=subprocess.DEVNULL, check=False).returncode == 0


def run_unit() -> int:
    """Offline helper checks: stamps, reordering, Codec2, Opus payloads."""
    import asyncio

    _fill_import_path()
    workdir = tempfile.mkdtemp(prefix="hermes-reticulum-unit-")
    os.environ.update({
        "RETICULUM_STORAGE": os.path.join(workdir, "storage"),
        "RETICULUM_STAMP_COST": "0",
        "RETICULUM_ALLOW_ALL_USERS": "true",
    })
    failures: list[str] = []

    def check(name: str, condition: bool, detail: str = "") -> None:
        print(f"unit: {'ok' if condition else 'FAIL'} {name}{(' — ' + detail) if detail else ''}",
              flush=True)
        if not condition:
            failures.append(name)

    from gateway.config import PlatformConfig
    from adapter import (  # noqa: E402
        AUDIO_MODE_OPUS_OGG as OPUS_MODE,
        ReticulumAdapter,
        _decode_codec2_to_ogg,
        _extract_audio_fields,
        _inbound_stamp,
        _outbound_stamp,
        _strip_part_tag,
    )

    now = time.time()
    check("outbound stamp", bool(now) and _outbound_stamp(now).endswith("Z] "))
    check("outbound multipart stamp", "2/5" in _outbound_stamp(now, 2, 5))
    check("inbound stamp flags late", "out of order" in _inbound_stamp(now - 60, now, True))
    check("part tag stripped", _strip_part_tag("[3/9] hi") == "hi")

    ogg_path = os.path.join(workdir, "tone.ogg")
    if _make_opus(ogg_path):
        class _Msg:
            fields = {0x07: [OPUS_MODE, open(ogg_path, "rb").read()]}
        paths, mimes, names, notes = _extract_audio_fields(_Msg(), 4 * 1024 * 1024)
        check("opus voice note extracted", bool(paths) and mimes == ["audio/ogg"] and not notes,
              f"{paths} {mimes} {notes}")
        adapter = ReticulumAdapter(PlatformConfig(enabled=True, extra={}))
        payload, error = adapter._prepare_voice_payload(ogg_path)
        check("voice payload is ogg/opus", bool(payload) and payload[:4] == b"OggS" and not error,
              error)
    else:
        print("unit: skip audio checks (ffmpeg missing)", flush=True)

    ffmpeg = shutil.which("ffmpeg")
    if ffmpeg:
        codec2_path = os.path.join(workdir, "tone.c2")
        made = subprocess.run(
            [ffmpeg, "-v", "error", "-y", "-f", "lavfi",
             "-i", "sine=frequency=440:duration=1:sample_rate=8000",
             "-c:a", "libcodec2", "-mode", "2400", "-f", "codec2raw", codec2_path],
            capture_output=True, stdin=subprocess.DEVNULL, check=False).returncode == 0
        if made:
            decoded = _decode_codec2_to_ogg(open(codec2_path, "rb").read(), 0x08)
            check("codec2 2400 decoded to opus", bool(decoded) and open(decoded, "rb").read(4) == b"OggS")
            if decoded:
                os.unlink(decoded)
        else:
            print("unit: skip codec2 check (encoder unavailable)", flush=True)

    async def reorder_check() -> tuple[list[float], dict[int, bool]]:
        adapter = ReticulumAdapter(PlatformConfig(enabled=True, extra={}))
        adapter.reorder_window = 0.05
        adapter._running = True
        adapter._loop = asyncio.get_running_loop()
        seen: list[float] = []
        late: dict[int, bool] = {}

        async def _fake(text, source, timestamp, meta, *, received_at=None, out_of_order=False):
            seen.append(timestamp)
            late[len(seen)] = out_of_order

        adapter._dispatch_message = _fake  # type: ignore[assignment]
        records = [
            {"source": "aa", "text": "third", "timestamp": now + 3, "received_at": now,
             "meta": {}, "message_id": None},
            {"source": "aa", "text": "first", "timestamp": now + 1, "received_at": now,
             "meta": {}, "message_id": None},
            {"source": "aa", "text": "second", "timestamp": now + 2, "received_at": now,
             "meta": {}, "message_id": None},
        ]
        for record in records:
            adapter._submit_inbound(dict(record))
        await asyncio.sleep(0.3)
        adapter._running = True
        adapter._submit_inbound({"source": "aa", "text": "late", "timestamp": now - 30,
                                 "received_at": now, "meta": {}, "message_id": None})
        await asyncio.sleep(0.3)
        return seen, late

    seen, late = asyncio.run(reorder_check())
    check("messages reordered by timestamp", seen[:3] == [now + 1, now + 2, now + 3], str(seen))
    check("late arrival flagged", late.get(4, False) is True, str(late))

    shutil.rmtree(workdir, ignore_errors=True)
    print("SELFTEST UNIT PASS" if not failures else f"SELFTEST UNIT FAIL: {failures}", flush=True)
    return 0 if not failures else 1


class _AnnouncePrinter:

    # RNS silently ignores announce handlers without this attribute.
    aspect_filter = None

    def __init__(self, tag):
        self.tag = tag

    def received_announce(self, destination_hash, announced_identity, app_data):
        print(f"{self.tag} saw announce from {destination_hash.hex()}", flush=True)


def run_client(workdir: str, port: int) -> int:
    import RNS
    import LXMF

    config_dir = os.path.join(workdir, "rns-client")
    _rns_config(config_dir, role="client", port=port)
    RNS.Reticulum(configdir=config_dir, loglevel=RNS.LOG_WARNING)
    RNS.Transport.register_announce_handler(_AnnouncePrinter("client"))

    router = LXMF.LXMRouter(storagepath=os.path.join(workdir, "storage-client"))
    identity = RNS.Identity()
    destination = router.register_delivery_identity(identity, display_name="selftest-client")
    received: list[dict] = []

    def _on_message(message):
        try:
            text = message.content_as_string() or ""
        except Exception:
            text = ""
        fields = message.fields or {}
        audio = fields.get(getattr(LXMF, "FIELD_AUDIO", 0x07))
        print(f"client received: {text!r} audio={bool(audio)} "
              f"signature_validated={message.signature_validated}", flush=True)
        received.append({"text": text, "audio": audio})

    router.register_delivery_callback(_on_message)

    server_file = os.path.join(workdir, "server.addr")
    deadline = time.time() + 30
    while time.time() < deadline and not os.path.exists(server_file):
        time.sleep(0.2)
    if not os.path.exists(server_file):
        print("client: server address file never appeared", flush=True)
        return 1
    with open(server_file, encoding="utf-8") as handle:
        server_hash = bytes.fromhex(handle.read().strip())

    time.sleep(4)  # let the TCP interface come up before the announce
    destination.announce()
    for _ in range(30):
        if RNS.Identity.recall(server_hash) and RNS.Transport.has_path(server_hash):
            break
        RNS.Transport.request_path(server_hash)
        time.sleep(1)
    print(f"client: server recalled={bool(RNS.Identity.recall(server_hash))} "
          f"has_path={RNS.Transport.has_path(server_hash)}", flush=True)
    if not RNS.Identity.recall(server_hash):
        print("client: could not recall the server identity", flush=True)
        return 1
    time.sleep(2)

    server_dest = RNS.Destination(
        RNS.Identity.recall(server_hash), RNS.Destination.OUT,
        RNS.Destination.SINGLE, "lxmf", "delivery")

    def _send(content: str, fields=None) -> None:
        message = LXMF.LXMessage(
            server_dest, destination, content, desired_method=LXMF.LXMessage.DIRECT,
            fields=fields)
        message.register_failed_callback(
            lambda lxm: print(f"client: message failed (state {lxm.state})", flush=True))
        router.handle_outbound(message)

    def _wait_for(predicate, timeout: float = 120) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if any(predicate(item) for item in received):
                return True
            time.sleep(0.5)
        return False

    _send("ping")
    if not _wait_for(lambda item: "echo:" in item["text"] and "ping" in item["text"]):
        print(f"client: no text reply {received!r}", flush=True)
        return 1
    text_reply = next(item["text"] for item in received
                      if "echo:" in item["text"] and "ping" in item["text"])
    if not text_reply.startswith("["):
        print(f"client: reply is not timestamped: {text_reply!r}", flush=True)
        return 1
    print(f"client: timestamped reply {text_reply!r}", flush=True)

    # Voice note in, spoken reply out.
    tone = os.path.join(workdir, "client-tone.ogg")
    if _make_opus(tone):
        with open(tone, "rb") as handle:
            audio = handle.read()
        _send("(voice note)", {getattr(LXMF, "FIELD_AUDIO", 0x07): [AUDIO_MODE_OPUS_OGG, audio]})
        if not _wait_for(lambda item: "voice-ok" in item["text"]):
            print(f"client: voice note was not accepted {received!r}", flush=True)
            return 1
        if not _wait_for(lambda item: bool(item["audio"])):
            print("client: no audio reply arrived", flush=True)
            return 1
        reply_audio = next(item["audio"] for item in received if item["audio"])
        ok_audio = (isinstance(reply_audio, (list, tuple)) and len(reply_audio) >= 2
                    and reply_audio[0] == AUDIO_MODE_OPUS_OGG
                    and bytes(reply_audio[1])[:4] == b"OggS")
        if not ok_audio:
            print(f"client: unexpected audio reply {reply_audio!r}", flush=True)
            return 1
        print(f"client: voice note reply received ({len(reply_audio[1])} bytes)", flush=True)
    else:
        print("client: skip voice note check (ffmpeg missing)", flush=True)

    print("SELFTEST CLIENT OK", flush=True)
    return 0


def run_server(workdir: str, port: int) -> int:
    import asyncio

    _fill_import_path()
    config_dir = os.path.join(workdir, "rns-server")
    _rns_config(config_dir, role="server", port=port)
    tone = os.path.join(workdir, "server-tone.ogg")
    _make_opus(tone, seconds=1.0)
    os.environ.update({
        "RETICULUM_STORAGE": os.path.join(workdir, "storage-server"),
        "RETICULUM_CONFIG": config_dir,
        "RETICULUM_DISPLAY_NAME": "selftest-server",
        "RETICULUM_ALLOW_ALL_USERS": "true",
        "RETICULUM_STAMP_COST": "0",
        "RETICULUM_DELIVERY_TIMEOUT": "60",
        "RETICULUM_PROPAGATED_FALLBACK": "false",
        "HERMES_RETICULUM_TEXT_BATCH_DELAY": "0",
        "RETICULUM_REORDER_WINDOW": "0.5",
    })

    async def main() -> int:
        from gateway.config import PlatformConfig
        from gateway.platforms.event import MessageType
        from adapter import ReticulumAdapter

        adapter = ReticulumAdapter(PlatformConfig(enabled=True, extra={}))
        done = asyncio.Event()
        result: dict[str, object] = {}

        async def echo(event):
            print(f"server inbound: type={event.message_type} media={event.media_urls} "
                  f"text={event.text!r} from {event.source.chat_id}", flush=True)
            if event.message_type == MessageType.VOICE and event.media_urls:
                result["voice_in"] = os.path.getsize(event.media_urls[0])
                sent = await adapter.send(event.source.chat_id, "voice-ok")
                result.update(voice_echo=sent.success, error=sent.error)
                voiced = await adapter.send_voice(
                    event.source.chat_id, tone, caption=None) if os.path.exists(tone) else None
                result.update(
                    voice_out=bool(voiced and voiced.success),
                    voice_error=getattr(voiced, "error", "no tone file"))
                done.set()
                return
            sent = await adapter.send(event.source.chat_id, f"echo: {event.text}")
            result.update(success=sent.success, error=sent.error)
            done.set()

        # Bypass only the gateway's batching queue; everything from the LXMF
        # callback through ordering, event construction, STT metadata and
        # send()/send_voice() is the real code path.
        adapter._enqueue_text_event = lambda event: asyncio.create_task(echo(event))

        if not await adapter.connect():
            print("server: adapter.connect() failed", flush=True)
            return 1
        import RNS
        RNS.Transport.register_announce_handler(_AnnouncePrinter("server"))
        print(f"server ready: {adapter.address}", flush=True)
        with open(os.path.join(workdir, "server.addr"), "w", encoding="utf-8") as handle:
            handle.write((adapter.address or "").strip("<>"))

        try:
            deadline = time.time() + 150
            while time.time() < deadline:
                if result.get("success") and result.get("voice_in") and result.get("voice_out"):
                    break
                try:
                    await asyncio.wait_for(done.wait(), timeout=5)
                except asyncio.TimeoutError:
                    continue
                done.clear()
            else:
                print("server: timed out waiting for the text + voice round trips", flush=True)
                return 1
        finally:
            with contextlib.suppress(Exception):
                await adapter.disconnect()
        print(f"SELFTEST SERVER result={result}", flush=True)
        ok = bool(result.get("success")) and bool(result.get("voice_in")) and bool(result.get("voice_out"))
        return 0 if ok else 1

    return asyncio.run(main())


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_for_file(path: str, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(path):
            return True
        time.sleep(0.2)
    return False


def main() -> int:
    if len(sys.argv) >= 2 and sys.argv[1] in {"server", "client", "unit"}:
        if sys.argv[1] == "unit":
            return run_unit()
        role, workdir, port = sys.argv[1], sys.argv[2], int(sys.argv[3])
        return run_server(workdir, port) if role == "server" else run_client(workdir, port)

    unit_rc = run_unit()
    if unit_rc != 0:
        print("SELFTEST FAIL (unit)", flush=True)
        return unit_rc

    workdir = os.environ.get("HERMES_SELFTEST_WORKDIR") or tempfile.mkdtemp(
        prefix="hermes-reticulum-selftest-")
    os.makedirs(workdir, exist_ok=True)
    port = _free_port()
    print(f"selftest workdir: {workdir} (port {port})", flush=True)
    logs = {name: open(os.path.join(workdir, f"{name}.log"), "w", encoding="utf-8")
            for name in ("server", "client")}
    try:
        server = subprocess.Popen(
            [sys.executable, os.path.abspath(__file__), "server", workdir, str(port)],
            stdout=logs["server"], stderr=subprocess.STDOUT)
        if not _wait_for_file(os.path.join(workdir, "server.addr"), 60):
            print("selftest: server did not become ready", flush=True)
            server.terminate()
            return 1
        client = subprocess.Popen(
            [sys.executable, os.path.abspath(__file__), "client", workdir, str(port)],
            stdout=logs["client"], stderr=subprocess.STDOUT)
        try:
            client_rc = client.wait(timeout=180)
            server_rc = server.wait(timeout=180)
        except subprocess.TimeoutExpired:
            print("selftest: timed out", flush=True)
            client.kill()
            server.kill()
            return 1
        for name, handle in logs.items():
            handle.flush()
            with open(os.path.join(workdir, f"{name}.log"), encoding="utf-8") as reader:
                print(f"--- {name} ---\n{reader.read().strip()}", flush=True)
        ok = client_rc == 0 and server_rc == 0
        print("SELFTEST PASS" if ok else "SELFTEST FAIL", flush=True)
        return 0 if ok else 1
    finally:
        for handle in logs.values():
            handle.close()
        if os.environ.get("KEEP_SELFTEST") != "1":
            shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
