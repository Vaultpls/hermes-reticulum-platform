#!/usr/bin/env python3
"""End-to-end self-test for the Reticulum/LXMF Hermes platform.

Runs two processes on loopback TCP:

* ``server`` boots the real ``ReticulumAdapter`` against a temporary RNS
  config and LXMF storage, then echoes every authorized inbound message.
* ``client`` is a bare LXMF peer that announces itself, sends ``ping`` to
  the adapter's address and waits for the echoed reply.

No public Reticulum transport nodes and no LoRa hardware are used. Run:

    python3 scripts/selftest.py

Requirements: the same Python environment Hermes uses (rns + lxmf installed)
and the Hermes source tree importable (set ``HERMES_SRC`` if it is not at
``~/.hermes/hermes-agent``). Exit code 0 means the full round trip worked.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_DIR = os.path.dirname(THIS_DIR)


def _fill_import_path() -> None:
    sys.path.insert(0, REPO_DIR)
    try:
        import gateway  # noqa: F401
    except ModuleNotFoundError:
        for candidate in (os.environ.get("HERMES_SRC"),
                          os.path.expanduser("~/.hermes/hermes-agent")):
            if candidate and os.path.isdir(os.path.join(candidate, "gateway")):
                sys.path.insert(0, candidate)
                return
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


class _AnnouncePrinter:

    def __init__(self, tag):
        self.tag = tag

    def received_announce(self, destination_hash, announced_identity, app_data):
        print(f"{self.tag} saw announce from {destination_hash.hex()}",
              flush=True)


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
    received: list[str] = []

    def _on_message(message):
        try:
            text = message.content_as_string() or ""
        except Exception:
            text = ""
        print(f"client received: {text!r} signature_validated={message.signature_validated}",
              flush=True)
        received.append(text)

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
    message = LXMF.LXMessage(
        server_dest, destination, "ping", desired_method=LXMF.LXMessage.DIRECT)
    message.register_failed_callback(
        lambda lxm: print(f"client: message failed (state {lxm.state})", flush=True))
    router.handle_outbound(message)

    deadline = time.time() + 120
    while time.time() < deadline and not received:
        time.sleep(0.5)
    if received and received[0].startswith("echo: ping"):
        print("SELFTEST CLIENT OK", flush=True)
        return 0
    print(f"client: unexpected result {received!r}", flush=True)
    return 1


def run_server(workdir: str, port: int) -> int:
    import asyncio

    _fill_import_path()
    config_dir = os.path.join(workdir, "rns-server")
    _rns_config(config_dir, role="server", port=port)
    os.environ.update({
        "RETICULUM_STORAGE": os.path.join(workdir, "storage-server"),
        "RETICULUM_CONFIG": config_dir,
        "RETICULUM_DISPLAY_NAME": "selftest-server",
        "RETICULUM_ALLOW_ALL_USERS": "true",
        "RETICULUM_STAMP_COST": "0",
        "RETICULUM_DELIVERY_TIMEOUT": "60",
        "RETICULUM_PROPAGATED_FALLBACK": "false",
        "HERMES_RETICULUM_TEXT_BATCH_DELAY": "0",
    })

    async def main() -> int:
        from gateway.config import PlatformConfig
        from adapter import ReticulumAdapter

        adapter = ReticulumAdapter(PlatformConfig(enabled=True, extra={}))
        done = asyncio.Event()
        result: dict[str, object] = {}

        async def echo(event):
            print(f"server inbound: {event.text!r} from {event.source.chat_id}", flush=True)
            sent = await adapter.send(event.source.chat_id, f"echo: {event.text}")
            result.update(success=sent.success, error=sent.error)
            done.set()

        # Bypass only the gateway's batching queue; everything from the LXMF
        # callback through event construction and send() is the real code path.
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
            await asyncio.wait_for(done.wait(), timeout=120)
        except asyncio.TimeoutError:
            print("server: timed out waiting for an inbound message", flush=True)
            await adapter.disconnect()
            return 1
        await adapter.disconnect()
        print(f"SELFTEST SERVER result={result}", flush=True)
        return 0 if result.get("success") else 1

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
    if len(sys.argv) >= 2 and sys.argv[1] in {"server", "client"}:
        role, workdir, port = sys.argv[1], sys.argv[2], int(sys.argv[3])
        return run_server(workdir, port) if role == "server" else run_client(workdir, port)

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
