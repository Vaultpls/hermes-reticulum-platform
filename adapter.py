"""Reticulum (LXMF) gateway platform adapter for Hermes Agent.

Presents Hermes as an LXMF delivery endpoint on a Reticulum mesh. Mesh
clients (Sideband, Nomad Network, RNode tooling, custom LXMF apps) send
encrypted messages to the agent's LXMF address; replies are delivered back
over the same mesh with delivery proof where the transport allows it.

Design notes
------------
* RNS is a process-wide singleton and LXMF keeps a SQLite store per router,
  so the Reticulum instance, router, identity and delivery destination are
  cached in ``_STACKS`` keyed by storage directory. Reconnects of the
  gateway adapter reuse that stack instead of re-initialising RNS (which
  raises on a second initialisation).
* LXMF callbacks fire on RNS daemon threads. Inbound messages are marshalled
  onto the gateway's asyncio loop; outbound ``handle_outbound()`` calls run
  on short-lived worker threads because LXMF serialises sends behind a lock
  that may be held for the duration of a mesh delivery attempt.
* Only messages whose Ed25519 signature validated are accepted. An unknown
  source identity cannot be verified, and LXMF lets a sender place an
  arbitrary ``source_hash`` in the envelope, so accepting unverified
  messages would let anyone spoof an allow-listed peer.

Configuration (environment wins over ``platforms.reticulum.extra``):

    RETICULUM_DISPLAY_NAME       announced name             (default "Hermes")
    RETICULUM_STORAGE            identity + router storage  (default ~/.lxmf/storage)
    RETICULUM_CONFIG             Reticulum config dir       (default ~/.reticulum)
    RETICULUM_STAMP_COST         inbound PoW stamp cost     (default 8; 0 disables)
    RETICULUM_ANNOUNCE_INTERVAL  re-announce seconds        (default 3600)
    RETICULUM_ALLOWED_USERS      comma-separated LXMF hashes
    RETICULUM_ALLOW_ALL_USERS    "true" opens the DM policy (not recommended)
    RETICULUM_PROPAGATION_NODE   outbound LXMF propagation node hash
    RETICULUM_PROPAGATED_FALLBACK  fall back to store-and-forward (default true)
    RETICULUM_DELIVERY_TIMEOUT   seconds to wait for DIRECT proof (default 150)
    RETICULUM_PROPAGATED_TIMEOUT seconds to wait for PN acceptance (default 180)
    RETICULUM_DENY_REPLY         reply "access denied" to unknown senders (default false)
    RETICULUM_HOME_CHANNEL[_NAME] default cron/notification destination
    HERMES_RETICULUM_TEXT_BATCH_DELAY  inbound text merge window (default 1.5)
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import json
import logging
import math
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
from collections import OrderedDict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Optional

from gateway.config import Platform, PlatformConfig
from gateway.platforms._shared import (
    decode_json_list_literal,
    get_scoped_secret,
    platform_gate_env,
    seed_extra_from_env,
    send_error,
)
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent, MessageType

logger = logging.getLogger(__name__)

try:  # package import (normal plugin load)
    from .telephony import TelephoneBridge
except ImportError:  # pragma: no cover - loaded as a top-level module
    from telephony import TelephoneBridge  # type: ignore[no-redef]

DEFAULT_DISPLAY_NAME = "Hermes"
DEFAULT_STORAGE_PATH = "~/.lxmf/storage"
DEFAULT_STAMP_COST = 8
DEFAULT_ANNOUNCE_INTERVAL = 3600.0
DEFAULT_DELIVERY_TIMEOUT = 150.0
DEFAULT_PROPAGATED_TIMEOUT = 180.0
DEFAULT_PROPAGATION_SYNC_INTERVAL = 60.0
DEFAULT_MAX_MESSAGE_LENGTH = 1000
DEFAULT_TEXT_BATCH_DELAY = 1.5
MIN_ANNOUNCE_INTERVAL = 3600.0  # RNS AR_TARGET: faster announces get rate-blocked
MAX_MESSAGE_LENGTH = 8000

# Voice (telephone-style use from Sideband and other LXMF phone clients).
# LXMF carries voice notes in FIELD_AUDIO as [mode, bytes]; Sideband plays
# AM_OPUS_OGG directly and decodes the Codec2 modes, so replies are always
# Ogg/Opus and inbound Codec2 is transcoded to Ogg/Opus before STT sees it.
DEFAULT_AUDIO_BITRATE = "24k"
DEFAULT_MAX_AUDIO_BYTES = 4 * 1024 * 1024      # inbound clip cap (before transcode)
DEFAULT_TTS_MAX_BYTES = 96 * 1024             # outbound voice-note cap (mesh budget)
DEFAULT_CALL_MAX_SECONDS = 300                # longest recorded LXST call
DEFAULT_REORDER_WINDOW = 1.0                  # seconds to hold for late arrivals
DEFAULT_TIMESTAMPS = True


def _is_call_request(text: str) -> bool:
    """Only standalone immediate requests trigger a call, never quoted prose."""
    return bool(re.fullmatch(
        r"\s*(?:please\s+)?(?:call me(?: back| up)?|give me a call|ring me|"
        r"phone me|dial me|give me a ring|"
        r"(?:can|could|would) you (?:call me(?: back| up)?|give me a call|"
        r"ring me|phone me|dial me|give me a ring))"
        r"(?:\s+(?:now|please))?[.!?]*\s*", text, re.IGNORECASE))


MAX_REORDER_WINDOW = 30.0
AUDIO_MODE_OPUS_OGG = 0x10
_CODEC2_FFMPEG_MODES = {
    0x03: "700C", 0x04: "1200", 0x05: "1300", 0x06: "1400",
    0x07: "1600", 0x08: "2400", 0x09: "3200",
}
_AUDIO_EXTS = frozenset({
    ".ogg", ".oga", ".opus", ".mp3", ".m4a", ".m2a", ".wav", ".flac",
    ".aac", ".caf", ".amr", ".3gp", ".webm",
})
_AUDIO_MIME_BY_EXT = {
    ".ogg": "audio/ogg", ".oga": "audio/ogg", ".opus": "audio/opus",
    ".mp3": "audio/mpeg", ".m4a": "audio/mp4", ".m2a": "audio/mpeg",
    ".wav": "audio/wav", ".flac": "audio/flac", ".aac": "audio/aac",
    ".caf": "audio/x-caf", ".amr": "audio/amr", ".3gp": "audio/3gpp",
    ".webm": "audio/webm",
}

_LXMF_DELIVERY_ASPECT = ("lxmf", "delivery")

# Process-wide stack cache: storage path -> _BridgeStack.
_STACKS: dict[str, "_BridgeStack"] = {}
_STACKS_LOCK = threading.RLock()

# Set while an adapter is connected so plugin tools can reach the live stack.
_LIVE_ADAPTER: Optional["ReticulumAdapter"] = None

def _max_stamp_cost() -> int:
    """Highest stamp cost LXMF will accept (cap is exclusive)."""
    try:
        import RNS
        return max(1, int(RNS.Transport.STAMP_COST_CAP) - 1)
    except Exception:
        return 63


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _deps_available() -> bool:
    """Passive, side-effect-free dependency probe (safe from status displays)."""
    return (importlib.util.find_spec("RNS") is not None
            and importlib.util.find_spec("LXMF") is not None)


def _truthy(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _env_or_extra(env_name: str, extra: dict, key: str, default: Any = None) -> Any:
    value = get_scoped_secret(env_name)
    if value is not None and str(value).strip() != "":
        return value
    value = extra.get(key)
    return default if value is None else value


def _as_int(value: Any, default: int, minimum: int = 0, maximum: int = 2**31 - 1) -> int:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, parsed))


def _as_float(value: Any, default: float) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return default


def _normalize_hash(value: Any) -> str:
    """Normalise an LXMF destination hash for comparison (lowercase hex)."""
    text = str(value or "").strip().lower()
    for char in ("<", ">", ":", " ", "-"):
        text = text.replace(char, "")
    return text


def _is_destination_hash(value: str) -> bool:
    return bool(re.fullmatch(r"[0-9a-f]{32}", value or ""))


def _short(hash_hex: str) -> str:
    return (hash_hex or "")[:8] or "<unknown>"


def _source_hash_of(message) -> Optional[str]:
    raw = getattr(message, "source_hash", None)
    if isinstance(raw, (bytes, bytearray)):
        return bytes(raw).hex()
    return _normalize_hash(raw)


def _utc_stamp(timestamp: float) -> str:
    """Human-readable UTC timestamp used in every message body (``2026-11-25 14:03:12Z``)."""
    return datetime.fromtimestamp(timestamp, tz=UTC).strftime("%Y-%m-%d %H:%M:%SZ")


def _outbound_stamp(timestamp: float, index: int = 0, total: int = 1) -> str:
    """Prefix for an outgoing part: ``[<utc>]`` or ``[<utc> 2/5]``.

    Every part carries the stamp so a client that reassembles a split reply out
    of order can still sort it, and so a human can see when it was sent.
    """
    head = _utc_stamp(timestamp)
    if total > 1:
        head = f"{head} {index}/{total}"
    return f"[{head}] "


def _inbound_stamp(timestamp: float, received_at: float, out_of_order: bool) -> str:
    """Prefix for an incoming message: its send time, flagged when it arrived late."""
    head = _utc_stamp(timestamp)
    if out_of_order:
        head = f"{head} | out of order, arrived {max(0.0, received_at - timestamp):.0f}s late"
    return f"[{head}] "


def _is_message_hash(value: Any) -> bool:
    return bool(re.fullmatch(r"[0-9a-f]{64}", str(value or "").strip().lower()))


def _audio_mime_for(name: str) -> Optional[str]:
    return _AUDIO_MIME_BY_EXT.get(Path(str(name or "")).suffix.lower())


def _decode_codec2_to_ogg(data: bytes, mode: int, bitrate: str = DEFAULT_AUDIO_BITRATE,
                          timeout: int = 120) -> Optional[str]:
    """Transcode a raw Codec2 voice frame stream to Ogg/Opus.

    Sideband's PTT default is Codec2 (2400 bps) to survive LoRa links, so most
    phone voice notes arrive that way. ffmpeg ships a ``codec2raw`` demuxer with
    libcodec2; when it is unavailable the clip is reported instead of being
    silently dropped.
    """
    ff_mode = _CODEC2_FFMPEG_MODES.get(int(mode))
    ffmpeg = shutil.which("ffmpeg")
    if not ff_mode or not ffmpeg or not data:
        return None
    fd, src_path = tempfile.mkstemp(prefix="lxmf_codec2_", suffix=".c2")
    os.close(fd)
    fd, out_path = tempfile.mkstemp(prefix="lxmf_codec2_", suffix=".ogg")
    os.close(fd)
    decoded = False
    try:
        with open(src_path, "wb") as handle:
            handle.write(data)
        result = subprocess.run(
            [ffmpeg, "-v", "error", "-y", "-f", "codec2raw", "-mode", ff_mode,
             "-i", src_path, "-acodec", "libopus", "-ac", "1", "-b:a", bitrate,
             "-application", "voip", "-f", "ogg", out_path],
            capture_output=True, timeout=timeout, stdin=subprocess.DEVNULL, check=False)
        if result.returncode == 0 and os.path.getsize(out_path) > 0:
            decoded = True
            return out_path
        logger.warning("Reticulum: Codec2 (%s) decode failed: %s", ff_mode,
                       (result.stderr or b"").decode("utf-8", errors="replace")[:300])
        return None
    except Exception as exc:
        logger.warning("Reticulum: Codec2 (%s) decode error: %s", ff_mode, exc)
        return None
    finally:
        with contextlib.suppress(OSError):
            os.unlink(src_path)
        if not decoded:
            with contextlib.suppress(OSError):
                os.unlink(out_path)


def _transcode_audio_file(path: str, bitrate: str = DEFAULT_AUDIO_BITRATE) -> Optional[str]:
    """Transcode any ffmpeg-readable audio file to Ogg/Opus, returning a temp path."""
    from gateway.platforms.base import transcode_to_ogg_opus
    return transcode_to_ogg_opus(path, bitrate=bitrate)


def _extract_audio_fields(message, max_bytes: int) -> tuple[list[str], list[str], list[str], list[str]]:
    """Save LXMF voice/audio attachments to the audio cache.

    Returns ``(paths, mime_types, file_names, notes)``; ``notes`` are
    human-readable lines for clips this host cannot decode so the agent (and
    the sender) learns why a voice note produced no transcript.
    """
    import LXMF
    from gateway.platforms.base import cache_audio_from_bytes

    paths: list[str] = []
    mimes: list[str] = []
    names: list[str] = []
    notes: list[str] = []
    fields = getattr(message, "fields", None) or {}
    if not isinstance(fields, dict):
        return paths, mimes, names, notes

    def _too_big(blob: "bytes | bytearray") -> bool:
        return len(blob) > max_bytes

    audio_field = fields.get(getattr(LXMF, "FIELD_AUDIO", 0x07))
    if isinstance(audio_field, (list, tuple)) and len(audio_field) >= 2:
        mode, blob = audio_field[0], audio_field[1]
        try:
            mode = int(mode)
        except (TypeError, ValueError):
            mode = -1
        if not isinstance(blob, (bytes, bytearray)) or not blob:
            pass
        elif _too_big(blob):
            notes.append(f"[voice note dropped: {len(blob)} bytes exceeds the {max_bytes} byte limit]")
        elif mode == AUDIO_MODE_OPUS_OGG:
            path = cache_audio_from_bytes(bytes(blob), ".ogg")
            paths.append(path)
            mimes.append("audio/ogg")
            names.append("voice-note.ogg")
        elif mode in _CODEC2_FFMPEG_MODES:
            ogg_path = _decode_codec2_to_ogg(bytes(blob), mode)
            if ogg_path:
                try:
                    with open(ogg_path, "rb") as handle:
                        path = cache_audio_from_bytes(handle.read(), ".ogg")
                    paths.append(path)
                    mimes.append("audio/ogg")
                    names.append("voice-note.ogg")
                finally:
                    with contextlib.suppress(OSError):
                        os.unlink(ogg_path)
            else:
                notes.append(
                    "[voice note in Codec2 mode could not be decoded on this host; "
                    "ask the sender to enable HQ voice (Opus) or type the message]")
        else:
            notes.append(f"[voice note in unsupported audio mode 0x{mode & 0xFF:02x}; ask the sender to resend as Opus]")

    attachments = fields.get(getattr(LXMF, "FIELD_FILE_ATTACHMENTS", 0x05))
    if isinstance(attachments, (list, tuple)):
        for item in attachments[:8]:
            if not isinstance(item, (list, tuple)) or len(item) < 2:
                continue
            name, blob = item[0], item[1]
            if isinstance(name, (bytes, bytearray)):
                name = bytes(name).decode("utf-8", errors="replace")
            name = str(name or "")
            mime = _audio_mime_for(name)
            if not mime or not isinstance(blob, (bytes, bytearray)) or not blob:
                continue
            if _too_big(blob):
                notes.append(f"[audio attachment {name} dropped: exceeds the {max_bytes} byte limit]")
                continue
            suffix = Path(name).suffix.lower() or ".ogg"
            path = cache_audio_from_bytes(bytes(blob), suffix)
            paths.append(path)
            mimes.append(mime)
            names.append(name or f"attachment{suffix}")
    return paths, mimes, names, notes


def _parse_hash_list(raw: Any) -> set[str]:
    """Parse ``RETICULUM_ALLOWED_USERS`` / ``extra.allowed_users`` into a hash set."""
    if isinstance(raw, str):
        decoded = decode_json_list_literal(raw)
        if isinstance(decoded, list):
            items = decoded
        else:
            items = raw.split(",")
    elif isinstance(raw, (list, tuple, set)):
        items = list(raw)
    else:
        return set()
    return {norm for item in items if (norm := _normalize_hash(item))}


def _invalid_hash_items(raw: Any) -> list[str]:
    """Return allowlist entries that do not normalise to a 32-hex LXMF hash."""
    if isinstance(raw, str):
        decoded = decode_json_list_literal(raw)
        items = decoded if isinstance(decoded, list) else raw.split(",")
    elif isinstance(raw, (list, tuple, set)):
        items = list(raw)
    else:
        return []
    bad = []
    for item in items:
        token = str(item or "").strip()
        if not token:
            continue
        if not _is_destination_hash(_normalize_hash(token)):
            bad.append(token)
    return bad


def _strip_part_tag(part: str) -> str:
    """Drop the ``[2/5] `` tag ``_split_message`` adds (the stamp carries its own index)."""
    return re.sub(r"^\[\d+/\d+\]\s*", "", part or "")


def _split_message(text: str, limit: int) -> list[str]:
    """Split long replies at word boundaries, tagging multi-part chunks."""
    if not text:
        return []
    if len(text) <= limit:
        return [text]
    # Reserve room for a "[nn/nn] " prefix so tagged chunks stay <= limit.
    budget = max(1, limit - 12)
    parts: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= budget:
            parts.append(remaining)
            break
        cut = budget
        space = remaining.rfind(" ", 0, budget + 1)
        if space > budget * 0.3:
            cut = space
        parts.append(remaining[:cut].rstrip())
        remaining = remaining[cut:].lstrip()
    if len(parts) > 1:
        total = len(parts)
        parts = [f"[{index}/{total}] {part}" for index, part in enumerate(parts, 1)]
    return parts


_MEDIA_TAG = re.compile(r"MEDIA:\s*\S+")
_ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")


def _strip_media_tags(content: str) -> tuple[str, int]:
    """Remove ``MEDIA:<path>`` directives; audio rides in LXMF's audio field instead."""
    paths = _MEDIA_TAG.findall(content or "")
    return _MEDIA_TAG.sub("", content or "").strip(), len(paths)


def _strip_markdown(text: str) -> str:
    """Reduce Markdown to plain text for mesh clients.

    Heading stripping is line-anchored so ``C#`` and ``#1`` survive.
    """
    text = re.sub(r"```\w*[ \t]*\n?", "", text)
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"__(.+?)__", r"\1", text)
    text = re.sub(r"(?<!\w)\*(?!\s)(.+?)(?<!\s)\*(?!\w)", r"\1", text)
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", r"\2", text)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", text)
    text = re.sub(r"(?m)^\s{0,3}#{1,6}\s+", "", text)
    return text


def _state_name(lxm) -> str:
    states = getattr(lxm.__class__, "states", None)
    try:
        if states and 0 <= lxm.state < len(states):
            return str(states[lxm.state])
    except Exception:
        pass
    return str(getattr(lxm, "state", "unknown"))


def _platform_enum() -> Platform:
    """Resolve the dynamic ``Platform`` member.

    In the gateway the plugin registration runs before adapter construction,
    so ``Platform("reticulum")`` succeeds. Direct construction (self-test,
    embedding) reaches this before registration; fall back to the same
    pseudo-member mechanism the enum uses for plugin platforms.
    """
    try:
        return Platform("reticulum")
    except ValueError:
        add = getattr(Platform, "_add_pseudo_member", None)
        if add is None:
            raise
        return add("reticulum")


# Register at import time so session/reply routing never sees an invalid
# Platform, regardless of whether an adapter instance exists yet.
with contextlib.suppress(Exception):
    _platform_enum()


# ---------------------------------------------------------------------------
# Process-wide RNS / LXMF stack
# ---------------------------------------------------------------------------

class _DeliveryAnnounceHandler:
    """Learn peer display names from ``lxmf.delivery`` announces."""

    aspect_filter = "lxmf.delivery"
    receive_path_responses = True

    def __init__(self, stack: "_BridgeStack"):
        self._stack = stack

    def received_announce(self, destination_hash, announced_identity, app_data):
        try:
            from LXMF.LXMF import display_name_from_app_data
            name = display_name_from_app_data(app_data)
            if name:
                self._stack.peer_names[bytes(destination_hash).hex()] = str(name)
        except Exception as exc:  # pragma: no cover - malformed announce
            logger.debug("Reticulum: could not decode announce display name: %s", exc)


class _PropagationNodeTracker:
    """Auto-select an outbound propagation node from live announces.

    Only installed when no node is explicitly configured; the adapter can
    turn auto-selection off, in which case store-and-forward replies are
    unavailable until an operator pins a node.
    """

    aspect_filter = "lxmf.propagation"
    receive_path_responses = False

    def __init__(self, stack: "_BridgeStack"):
        self._stack = stack

    def received_announce(self, destination_hash, announced_identity, app_data,
                          announce_packet_hash=None, is_path_response=False):
        try:
            stack = self._stack
            if not stack.auto_propagation_node or stack.configured_pn is not None:
                return
            if not isinstance(destination_hash, (bytes, bytearray)) or len(destination_hash) != 16:
                return
            if app_data:
                from LXMF.LXMF import pn_announce_data_is_valid
                if not pn_announce_data_is_valid(app_data):
                    return
            hashed = bytes(destination_hash)
            current = stack.router.get_outbound_propagation_node()
            if current is not None:
                # Sticky: keep the first selected node. Flapping between
                # propagation nodes breaks message sync (messages sit on the
                # node that received them and we only ever pull from the one
                # currently selected) and tears down the sync link.
                return
            stack.router.set_outbound_propagation_node(hashed)
            logger.info("Reticulum: auto-selected propagation node %s", hashed.hex())
        except Exception as exc:  # pragma: no cover - announce races
            logger.debug("Reticulum: propagation tracker error: %s", exc)


class _BridgeStack:
    """Reticulum instance + LXMF router + delivery identity for one storage dir."""

    def __init__(self, storage_path: str, rns_config: Optional[str],
                 display_name: str, stamp_cost: int):
        # RNS installs signal handlers at construction, so this must happen on
        # the process main thread. Enforced here (not just at call sites) so a
        # mis-threaded caller fails loudly instead of poisoning the singleton.
        if threading.current_thread() is not threading.main_thread():
            raise RuntimeError(
                "Reticulum stack must be constructed on the main thread "
                "(RNS installs SIGINT/SIGTERM handlers at startup)")
        import RNS
        import LXMF

        self.storage_path = storage_path
        self.peer_names: dict[str, str] = {}
        self.sink = None  # callable(message) -> None, set by the live adapter
        self.configured_pn: Optional[bytes] = None
        self.auto_propagation_node = True

        existing = RNS.Reticulum.get_instance()
        if existing is None:
            kwargs = {"configdir": rns_config} if rns_config else {}
            RNS.Reticulum(**kwargs)
        elif rns_config:
            active = getattr(RNS.Reticulum, "configdir", None)
            if active and os.path.realpath(active) != os.path.realpath(os.path.expanduser(rns_config)):
                logger.warning(
                    "Reticulum: RNS already initialised with %s; ignoring RETICULUM_CONFIG=%s",
                    active, rns_config)

        try:
            Path(storage_path).mkdir(parents=True, exist_ok=True)
            os.chmod(storage_path, 0o700)
        except OSError as exc:
            logger.warning("Reticulum: could not secure storage dir %s: %s", storage_path, exc)
        identity_path = Path(storage_path) / "hermes_identity"
        identity = None
        if identity_path.exists():
            try:
                identity = RNS.Identity.from_file(str(identity_path))
            except Exception as exc:
                logger.warning("Reticulum: could not load identity %s: %s", identity_path, exc)
            if identity is None:
                backup = identity_path.with_name(
                    f"{identity_path.name}.corrupt-{int(time.time())}")
                with contextlib.suppress(OSError):
                    shutil.move(str(identity_path), str(backup))
                    os.chmod(backup, 0o600)
                logger.warning("Reticulum: identity unreadable; moved aside to %s", backup)
            else:
                try:
                    os.chmod(identity_path, 0o600)
                except OSError as exc:
                    logger.warning("Reticulum: could not tighten %s permissions: %s", identity_path, exc)

        if identity is None:
            identity = RNS.Identity()
            identity.to_file(str(identity_path))
            with contextlib.suppress(OSError):
                os.chmod(identity_path, 0o600)
            logger.info("Reticulum: created new LXMF identity at %s", identity_path)

        self.identity = identity
        stamp_cost = int(stamp_cost or 0)
        self.router = LXMF.LXMRouter(storagepath=storage_path, enforce_stamps=stamp_cost > 0)
        self.destination = self.router.register_delivery_identity(
            identity,
            display_name=display_name,
            stamp_cost=stamp_cost if stamp_cost and stamp_cost > 0 else None,
        )
        if self.destination is None:
            raise RuntimeError("LXMF refused to register a delivery identity")
        self.router.register_delivery_callback(self._deliver)
        self.address_hex = bytes(self.destination.hash).hex()

        RNS.Transport.register_announce_handler(_DeliveryAnnounceHandler(self))
        RNS.Transport.register_announce_handler(_PropagationNodeTracker(self))
        logger.info(
            "Reticulum: LXMF stack ready at %s — address %s (%s), stamp cost %s",
            storage_path, self.address_hex, display_name,
            stamp_cost if stamp_cost and stamp_cost > 0 else "off")

    def set_configured_propagation_node(self, node_hex: Optional[str]) -> None:
        if not node_hex:
            return
        try:
            node = bytes.fromhex(_normalize_hash(node_hex))
        except ValueError:
            logger.warning("Reticulum: invalid propagation node hash %r", node_hex)
            return
        if len(node) != 16:
            logger.warning("Reticulum: propagation node hash must be 32 hex chars; got %d", len(node))
            return
        self.configured_pn = node
        try:
            self.router.set_outbound_propagation_node(node)
            logger.info("Reticulum: outbound propagation node %s", node.hex())
        except Exception as exc:
            logger.warning("Reticulum: could not set propagation node: %s", exc)

    def announce(self) -> None:
        self.router.announce(self.destination.hash)

    def request_propagation_sync(self) -> bool:
        """Ask the selected propagation node for LXMF addressed to us.

        Sideband/NomadNet call this periodically; without it, messages a
        sender pushed to the propagation node sit there forever. Returns True
        when a sync was requested.
        """
        if self.router.get_outbound_propagation_node() is None:
            return False
        try:
            self.router.request_messages_from_propagation_node(self.identity)
            return True
        except Exception as exc:
            logger.warning("Reticulum: propagation sync request failed: %s", exc)
            return False

    def _deliver(self, message) -> None:
        sink = self.sink
        if sink is None:
            return
        try:
            sink(message)
        except Exception:
            logger.exception("Reticulum: inbound delivery sink failed")


def _get_stack(*, storage_path: str, rns_config: Optional[str],
               display_name: str, stamp_cost: int) -> _BridgeStack:
    """Return (creating on first use) the process-wide stack for *storage_path*."""
    key = os.path.realpath(os.path.expanduser(storage_path))
    with _STACKS_LOCK:
        stack = _STACKS.get(key)
        if stack is None:
            stack = _BridgeStack(key, rns_config, display_name, stamp_cost)
            _STACKS[key] = stack
        return stack


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class ReticulumAdapter(BasePlatformAdapter):
    """Hermes gateway adapter for LXMF over Reticulum."""

    MAX_MESSAGE_LENGTH = DEFAULT_MAX_MESSAGE_LENGTH
    # Inbound authorization is enforced here (LXMF has no bot/group concepts),
    # so the central authz mixin defers to _dm_policy / _is_dm_allowed.
    enforces_own_access_policy = True

    def __init__(self, config: PlatformConfig, **kwargs):
        super().__init__(config=config, platform=_platform_enum())
        extra = getattr(config, "extra", {}) or {}

        self.display_name = str(_env_or_extra(
            "RETICULUM_DISPLAY_NAME", extra, "display_name", DEFAULT_DISPLAY_NAME)).strip() or DEFAULT_DISPLAY_NAME
        storage = str(_env_or_extra(
            "RETICULUM_STORAGE", extra, "storage_path", DEFAULT_STORAGE_PATH)).strip()
        self.storage_path = os.path.expanduser(storage or DEFAULT_STORAGE_PATH)
        rns_config = _env_or_extra("RETICULUM_CONFIG", extra, "rns_config", None)
        self.rns_config = os.path.expanduser(str(rns_config)) if rns_config else None

        self.stamp_cost = _as_int(
            _env_or_extra("RETICULUM_STAMP_COST", extra, "stamp_cost", DEFAULT_STAMP_COST),
            DEFAULT_STAMP_COST, minimum=0, maximum=64)
        self.announce_interval = max(MIN_ANNOUNCE_INTERVAL, _as_float(
            _env_or_extra("RETICULUM_ANNOUNCE_INTERVAL", extra, "announce_interval",
                          DEFAULT_ANNOUNCE_INTERVAL), DEFAULT_ANNOUNCE_INTERVAL))
        self.direct_timeout = max(10.0, _as_float(
            _env_or_extra("RETICULUM_DELIVERY_TIMEOUT", extra, "delivery_timeout",
                          DEFAULT_DELIVERY_TIMEOUT), DEFAULT_DELIVERY_TIMEOUT))
        self.propagated_timeout = max(10.0, _as_float(
            _env_or_extra("RETICULUM_PROPAGATED_TIMEOUT", extra, "propagated_timeout",
                          DEFAULT_PROPAGATED_TIMEOUT), DEFAULT_PROPAGATED_TIMEOUT))
        self.propagation_sync_interval = max(0.0, _as_float(
            _env_or_extra("RETICULUM_PROPAGATION_SYNC_INTERVAL", extra,
                          "propagation_sync_interval", DEFAULT_PROPAGATION_SYNC_INTERVAL),
            DEFAULT_PROPAGATION_SYNC_INTERVAL))
        self.propagated_fallback = _truthy(
            _env_or_extra("RETICULUM_PROPAGATED_FALLBACK", extra, "propagated_fallback", True), True)
        self.auto_propagation_node = _truthy(
            _env_or_extra("RETICULUM_AUTO_PROPAGATION_NODE", extra, "auto_propagation_node", True), True)
        self.deny_reply = _truthy(
            _env_or_extra("RETICULUM_DENY_REPLY", extra, "deny_reply", False), False)
        self.propagation_node = _env_or_extra(
            "RETICULUM_PROPAGATION_NODE", extra, "outbound_propagation_node", None)

        self.max_message_length = _as_int(
            extra.get("max_message_length", DEFAULT_MAX_MESSAGE_LENGTH),
            DEFAULT_MAX_MESSAGE_LENGTH, minimum=200, maximum=MAX_MESSAGE_LENGTH)

        batch_delay = _as_float(
            os.getenv("HERMES_RETICULUM_TEXT_BATCH_DELAY", DEFAULT_TEXT_BATCH_DELAY),
            DEFAULT_TEXT_BATCH_DELAY)
        self._text_batch_delay_seconds = max(0.0, batch_delay)
        self._text_batch_split_delay_seconds = self._text_batch_delay_seconds

        # Timestamps ride in every outbound body and inbound bodies are stamped
        # with the sender's own LXMF timestamp, so a phone client that receives
        # parts out of order can still reconstruct the sequence.
        self.timestamps = _truthy(
            _env_or_extra("RETICULUM_TIMESTAMPS", extra, "timestamps", DEFAULT_TIMESTAMPS),
            DEFAULT_TIMESTAMPS)
        # Late-arrival buffer: hold inbound messages briefly, sort them by their
        # LXMF timestamps, then dispatch. 0 disables reordering (stamps still
        # make the true order readable, the message is just flagged as late).
        self.reorder_window = min(MAX_REORDER_WINDOW, max(0.0, _as_float(
            _env_or_extra("RETICULUM_REORDER_WINDOW", extra, "reorder_window",
                          DEFAULT_REORDER_WINDOW), DEFAULT_REORDER_WINDOW)))
        self.reorder_max_batch = _as_int(
            extra.get("reorder_max_batch", 16), 16, minimum=1, maximum=128)

        # Voice notes (telephone-style: talk to the agent, hear a reply).
        self.audio_bitrate = str(_env_or_extra(
            "RETICULUM_AUDIO_BITRATE", extra, "audio_bitrate", DEFAULT_AUDIO_BITRATE))
        self.max_audio_bytes = _as_int(
            _env_or_extra("RETICULUM_MAX_AUDIO_BYTES", extra, "max_audio_bytes",
                          DEFAULT_MAX_AUDIO_BYTES),
            DEFAULT_MAX_AUDIO_BYTES, minimum=64 * 1024, maximum=64 * 1024 * 1024)
        self.tts_max_bytes = _as_int(
            _env_or_extra("RETICULUM_TTS_MAX_BYTES", extra, "tts_max_bytes",
                          DEFAULT_TTS_MAX_BYTES),
            DEFAULT_TTS_MAX_BYTES, minimum=8 * 1024, maximum=16 * 1024 * 1024)

        # Telephone (LXST): answer allow-listed callers, record the call, and
        # hand the transcript to the agent. Optional: needs the lxst package.
        self.telephone_enabled = _truthy(
            _env_or_extra("RETICULUM_TELEPHONE", extra, "telephone", True), True)
        self.call_max_seconds = _as_int(
            _env_or_extra("RETICULUM_CALL_MAX_SECONDS", extra, "call_max_seconds",
                          DEFAULT_CALL_MAX_SECONDS),
            DEFAULT_CALL_MAX_SECONDS, minimum=30, maximum=3600)
        self.call_greeting = _env_or_extra(
            "RETICULUM_CALL_GREETING", extra, "call_greeting", None)
        self.call_live_transcription = _truthy(
            _env_or_extra("RETICULUM_CALL_LIVE", extra, "call_live", True), True)

        self._stack: Optional[_BridgeStack] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._telephone: Optional[TelephoneBridge] = None
        self._telephone_missing_logged = False
        self._next_telephone_attempt = 0.0
        self._announce_thread: Optional[threading.Thread] = None
        self._announce_stop = threading.Event()
        self._propagation_sync_task: Optional[asyncio.Task] = None
        self._running = False
        self._address_hex: Optional[str] = None
        self._denial_times: dict[str, float] = {}
        # Inbound sequencing: per-source pending records, a quiet-period task,
        # the last send timestamp already handed to the agent, and a dedupe
        # ledger (LXMF can deliver the same envelope twice through DIRECT and
        # store-and-forward).
        self._order_seq = 0
        self._order_pending: dict[str, list[dict[str, Any]]] = {}
        self._order_tasks: dict[str, asyncio.Task] = {}
        self._last_sent_at: dict[str, float] = {}
        self._seen_messages: "OrderedDict[tuple[str, str], float]" = OrderedDict()
        self._seen_messages_ttl = 900.0
        # Bound once so sink identity comparison in disconnect() is reliable.
        self._sink_callback = self._on_lxmf_message

        if self._allow_all():
            self._dm_policy = "open"
        elif self._allowed_users():
            self._dm_policy = "allowlist"
        else:
            self._dm_policy = "disabled"
        self._group_policy = "disabled"

        extra = getattr(self.config, "extra", {}) or {}
        raw_allow = platform_gate_env("RETICULUM_ALLOWED_USERS") or extra.get("allowed_users", [])
        bad_entries = _invalid_hash_items(raw_allow)
        if bad_entries:
            logger.warning(
                "Reticulum: %d allowlist entries are not 32-hex LXMF hashes and will never match: %s",
                len(bad_entries), ", ".join(_short(item) for item in bad_entries[:5]))

        logger.info(
            "Reticulum adapter initialized: name=%s storage=%s stamp=%s allow_all=%s "
            "allowed=%d propagation_node=%s fallback=%s timestamps=%s reorder=%.1fs "
            "voice=%s telephone=%s",
            self.display_name, self.storage_path,
            self.stamp_cost if self.stamp_cost else "off",
            self._allow_all(), len(self._allowed_users()),
            self.propagation_node or ("auto" if self.auto_propagation_node else "none"),
            self.propagated_fallback, self.timestamps, self.reorder_window,
            f"opus@{self.audio_bitrate}",
            f"on (max {self.call_max_seconds}s)" if self.telephone_enabled else "off")

    # -- address / display ---------------------------------------------------

    @property
    def address(self) -> Optional[str]:
        """Human-readable LXMF address of this endpoint, if connected."""
        return self._address_hex

    # -- stamp control -------------------------------------------------------

    def stamp_status(self) -> dict[str, Any]:
        """Current inbound stamp requirement, as seen by peers and the router."""
        stack = self._stack
        connected = bool(self._running and stack is not None)
        enforcing = bool(getattr(stack.router, "_enforce_stamps", False)) if connected else False
        return {
            "connected": connected,
            "address": self._address_hex,
            "stamp_cost": self.stamp_cost,
            "enforcing_stamps": enforcing,
            "max_stamp_cost": _max_stamp_cost(),
        }

    def set_stamp_cost(self, cost: int) -> dict[str, Any]:
        """Change the inbound proof-of-work requirement without a restart.

        0 disables stamps (announce advertises none, router stops validating),
        anything else requires that many leading zero bits. The new value is
        announced so peers stop wasting work (or start proving it) promptly.
        """
        try:
            value = int(cost)
        except (TypeError, ValueError):
            raise ValueError("stamp cost must be an integer")
        value = max(0, min(value, _max_stamp_cost()))
        stack = self._stack
        if stack is None:
            self.stamp_cost = value
            status = self.stamp_status()
            status["note"] = "adapter not connected; value applies on next connect"
            return status
        ok = stack.router.set_inbound_stamp_cost(
            stack.destination.hash, value if value > 0 else None)
        if not ok:
            raise RuntimeError("LXMF refused the stamp cost change")
        if value > 0:
            stack.router.enforce_stamps()
        else:
            stack.router.ignore_stamps()
        self.stamp_cost = value
        announced = True
        try:
            stack.announce()
        except Exception as exc:
            announced = False
            logger.warning("Reticulum: announce after stamp change failed: %s", exc)
        status = self.stamp_status()
        status["announced"] = announced
        logger.info("Reticulum: stamp cost set to %s (enforcing=%s, announced=%s)",
                    value, value > 0, announced)
        return status

    # -- display name control ------------------------------------------------

    def name_status(self) -> dict[str, Any]:
        stack = self._stack
        connected = bool(self._running and stack is not None)
        return {
            "connected": connected,
            "address": self._address_hex,
            "display_name": self.display_name,
        }

    def set_display_name(self, name: str) -> dict[str, Any]:
        """Rename the endpoint as announced on the mesh, without a restart."""
        clean = str(name).strip()
        if not clean:
            raise ValueError("display name must not be empty")
        encoded = clean.encode("utf-8")
        if len(encoded) > 64:
            raise ValueError("display name must be 64 bytes or fewer in UTF-8")
        stack = self._stack
        if stack is None:
            self.display_name = clean
            status = self.name_status()
            status["note"] = "adapter not connected; value applies on next connect"
            return status
        # The destination's announce app-data getter reads display_name live,
        # so updating the attribute is enough; re-announce to publish it.
        stack.destination.display_name = clean
        self.display_name = clean
        announced = True
        try:
            stack.announce()
        except Exception as exc:
            announced = False
            logger.warning("Reticulum: announce after name change failed: %s", exc)
        status = self.name_status()
        status["announced"] = announced
        logger.info("Reticulum: display name set to %r (announced=%s)", clean, announced)
        return status

    # -- access control ------------------------------------------------------

    def _allowed_users(self) -> set[str]:
        """Live allowlist read (env first, then config) so revokes apply at once."""
        extra = getattr(self.config, "extra", {}) or {}
        raw = platform_gate_env("RETICULUM_ALLOWED_USERS") or extra.get("allowed_users", [])
        return _parse_hash_list(raw)

    def _allow_all(self) -> bool:
        extra = getattr(self.config, "extra", {}) or {}
        value = platform_gate_env("RETICULUM_ALLOW_ALL_USERS")
        if not value:
            value = extra.get("allow_all_users", False)
        return _truthy(value)

    def _is_allowed(self, source_hash: str) -> bool:
        norm = _normalize_hash(source_hash)
        if not norm:
            return False
        if self._allow_all():
            return True
        return norm in self._allowed_users()

    def _is_dm_allowed(self, user_id: str) -> bool:
        """Live re-check used by the authz mixin after pairing revokes."""
        return self._is_allowed(user_id)

    # -- lifecycle -----------------------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not _deps_available():
            logger.error("Reticulum: RNS/LXMF are not installed in the Hermes runtime")
            self._set_fatal_error(
                "deps_missing",
                "Install the Reticulum stack: pip install rns lxmf",
                retryable=False)
            return False

        self._loop = asyncio.get_running_loop()
        try:
            # RNS.Reticulum() installs SIGINT handlers, so it must be created on
            # the main thread — never inside asyncio.to_thread().
            stack = _get_stack(
                storage_path=self.storage_path,
                rns_config=self.rns_config,
                display_name=self.display_name,
                stamp_cost=self.stamp_cost,
            )
        except Exception as exc:
            logger.error("Reticulum: connect failed: %s", exc, exc_info=True)
            self._set_fatal_error("connect_failed", str(exc), retryable=True)
            return False

        global _LIVE_ADAPTER
        self._stack = stack
        _LIVE_ADAPTER = self
        if stack.sink is not None and stack.sink is not self._sink_callback:
            logger.warning(
                "Reticulum: another adapter is already consuming inbound messages from "
                "storage=%s; only one live adapter per storage path is supported",
                self.storage_path)
        self._running = True
        stack.sink = self._sink_callback
        stack.configured_pn = None
        stack.auto_propagation_node = self.auto_propagation_node
        stack.set_configured_propagation_node(self.propagation_node)
        self._start_telephone(stack)

        import RNS
        self._address_hex = RNS.prettyhexrep(stack.destination.hash)
        try:
            await asyncio.to_thread(stack.announce)
        except Exception as exc:
            logger.warning("Reticulum: initial announce failed: %s", exc)

        self._mark_connected()
        self._start_announce_loop()
        self._start_propagation_sync_loop()
        logger.info(
            "Reticulum: connected — LXMF address %s (%s), re-announce every %.0fs",
            self._address_hex, self.display_name, self.announce_interval)
        return True

    async def disconnect(self) -> None:
        global _LIVE_ADAPTER
        self._running = False
        if _LIVE_ADAPTER is self:
            _LIVE_ADAPTER = None
        self._stop_announce_loop()
        self._stop_propagation_sync_loop()
        if self._telephone is not None:
            with contextlib.suppress(Exception):
                self._telephone.stop()
            self._telephone = None
        # Drop any partially held reorder batches: a reconnect must not replay
        # messages captured before the disconnect under a stale timer.
        for task in list(self._order_tasks.values()):
            if task is not None and not task.done():
                task.cancel()
        self._order_tasks.clear()
        self._order_pending.clear()
        if self._stack is not None and self._stack.sink is self._sink_callback:
            self._stack.sink = None
        self._mark_disconnected()
        logger.info("Reticulum: adapter disconnected (RNS stack retained)")

    # -- telephone -----------------------------------------------------------

    def _start_telephone(self, stack: "_BridgeStack") -> None:
        """Announce an LXST telephone destination and answer allowed callers."""
        if not self.telephone_enabled:
            logger.info("Reticulum: telephone disabled (RETICULUM_TELEPHONE=false)")
            return
        try:
            import importlib.util
            if importlib.util.find_spec("LXST") is None:
                if not self._telephone_missing_logged:
                    logger.info(
                        "Reticulum: telephone needs the optional lxst package; "
                        "run `pip install lxst` in the gateway environment "
                        "(the adapter retries periodically)")
                    self._telephone_missing_logged = True
                return
            bridge = TelephoneBridge(
                stack.identity,
                storage_path=self.storage_path,
                allowed=lambda caller: self._is_allowed(caller),
                on_transcript=self._on_call_transcript,
                on_utterance=self._on_call_utterance if self.call_live_transcription else None,
                greeting_path=self.call_greeting,
                max_call_seconds=self.call_max_seconds,
                live_transcription=self.call_live_transcription,
            )
            if bridge.start():
                self._telephone = bridge
            else:
                logger.info(
                    "Reticulum: telephone unavailable — install the optional lxst "
                    "package (pip install lxst) and restart the gateway")
        except Exception:
            logger.exception("Reticulum: telephone bridge failed to start")

    def _on_call_utterance(self, call: dict) -> None:
        """Dispatch one in-call utterance without waiting for hangup."""
        caller = str(call.get("caller") or "").strip().lower()
        reply_to = str(call.get("reply_to") or "").strip().lower()
        if not _is_destination_hash(reply_to):
            reply_to = caller
        if not _is_destination_hash(reply_to):
            logger.warning("Reticulum: dropping live call utterance with invalid caller %r", caller)
            return
        transcript = str(call.get("transcript") or "").strip()
        if not transcript:
            return
        duration = float(call.get("utterance_duration") or 0.0)
        index = int(call.get("utterance_index") or 0)
        now = time.time()
        record = {
            "source": reply_to,
            "text": f"[voice call, live {duration:.1f}s]\n{transcript}",
            "timestamp": now,
            "received_at": now,
            "meta": {
                "lxmf_source": reply_to,
                "call_identity": caller,
                "signature_validated": True,
                "call": True,
                "call_live": True,
                "call_utterance": index,
                "call_duration": duration,
                "call_recording": call.get("path"),
                "call_utterance_audio": call.get("utterance_path"),
                "media_urls": [],
                "media_types": [],
                "media_filenames": [],
            },
            "message_id": f"{reply_to}:call:live:{index}",
            "bypass_reorder": True,
        }
        loop = self._loop
        if loop is None or loop.is_closed():
            logger.warning("Reticulum: live call utterance from %s dropped (no event loop)", caller)
            return
        loop.call_soon_threadsafe(self._submit_inbound, record)

    def _on_call_transcript(self, call: dict) -> None:
        """Turn a finished call into an ordinary inbound turn for the agent."""
        caller = str(call.get("caller") or "").strip().lower()
        if not _is_destination_hash(caller):
            logger.warning("Reticulum: dropping call transcript with invalid caller %r", caller)
            return
        if call.get("live"):
            logger.info(
                "Reticulum: call with %s ended after %d live utterance(s); "
                "no duplicate end-of-call turn",
                caller, int(call.get("utterances") or 0))
            return
        # Prefer the LXMF delivery address derived from the caller's identity;
        # the telephony identity hash itself is not deliverable over LXMF.
        reply_to = str(call.get("reply_to") or "").strip().lower()
        if not _is_destination_hash(reply_to):
            reply_to = caller
        if reply_to != caller:
            logger.info("Reticulum: reply address for call from %s is %s", caller, reply_to)
        duration = float(call.get("duration") or 0.0)
        transcript = str(call.get("transcript") or "").strip()
        error = str(call.get("error") or "").strip()
        if transcript:
            body = f"[voice call, {duration:.0f}s]\n{transcript}"
        else:
            body = (f"[voice call, {duration:.0f}s — no transcript: "
                    f"{error or 'no audio captured'}]")
        now = time.time()
        record = {
            "source": reply_to,
            "text": body,
            "timestamp": now,
            "received_at": now,
            "meta": {
                "lxmf_source": reply_to,
                "call_identity": caller,
                "signature_validated": True,
                "call": True,
                "call_duration": duration,
                "call_recording": call.get("path"),
                "media_urls": [],
                "media_types": [],
                "media_filenames": [],
            },
            "message_id": None,
        }
        loop = self._loop
        if loop is None or loop.is_closed():
            logger.warning("Reticulum: call transcript from %s dropped (no event loop)", caller)
            return
        loop.call_soon_threadsafe(self._submit_inbound, record)

    # -- announce loop -------------------------------------------------------

    def _start_announce_loop(self) -> None:
        if self._announce_thread is not None and self._announce_thread.is_alive():
            return
        self._announce_stop.clear()
        self._announce_thread = threading.Thread(
            target=self._announce_job, name="reticulum-announce", daemon=True)
        self._announce_thread.start()

    def _announce_job(self) -> None:
        """Announce from a dedicated thread.

        The asyncio version silently stopped after its first pass, which
        left the telephone destination unannounced until the next restart.
        """
        first = True
        while self._running and not self._announce_stop.is_set():
            if self._announce_stop.wait(6.0 if first else self.announce_interval):
                break
            first = False
            if not self._running or self._stack is None:
                continue
            try:
                self._stack.announce()
                if self._telephone is not None:
                    self._telephone.announce()
                elif (self.telephone_enabled
                      and time.time() >= self._next_telephone_attempt):
                    # lxst can appear after startup (package installs, env
                    # rebuilds); retry without requiring a gateway restart.
                    self._next_telephone_attempt = time.time() + 600.0
                    self._start_telephone(self._stack)
                suffix = ""
                if self._telephone is not None:
                    suffix = f" + telephone {self._telephone.address or '?'}"
                up = -1
                try:
                    import RNS

                    up = sum(1 for iface in RNS.Transport.interfaces
                             if getattr(iface, "online", False))
                except Exception:
                    pass
                logger.info("Reticulum: announced %s%s (%d interfaces up)",
                            self._address_hex, suffix, up)
            except Exception as exc:
                logger.warning("Reticulum: re-announce failed: %s", exc)

    def _stop_announce_loop(self) -> None:
        self._announce_stop.set()
        self._announce_thread = None

    async def _propagation_sync_loop(self) -> None:
        # Give paths and the first auto-selected propagation node ~20s to
        # settle before the first pull, then sync on the configured interval.
        try:
            await asyncio.sleep(20)
            while self._running:
                if self._stack is not None:
                    try:
                        await asyncio.to_thread(self._stack.request_propagation_sync)
                    except Exception as exc:
                        logger.debug("Reticulum: propagation sync failed: %s", exc)
                await asyncio.sleep(self.propagation_sync_interval)
        except asyncio.CancelledError:
            pass

    def _start_propagation_sync_loop(self) -> None:
        if self.propagation_sync_interval <= 0:
            logger.info("Reticulum: propagation sync disabled (RETICULUM_PROPAGATION_SYNC_INTERVAL=0)")
            return
        if self._loop is None or self._loop.is_closed():
            return
        if self._propagation_sync_task is not None and not self._propagation_sync_task.done():
            return
        self._propagation_sync_task = self._loop.create_task(self._propagation_sync_loop())

    def _stop_propagation_sync_loop(self) -> None:
        task, self._propagation_sync_task = self._propagation_sync_task, None
        if task is not None and not task.done():
            task.cancel()

    # -- inbound -------------------------------------------------------------

    def _on_lxmf_message(self, message) -> None:
        """LXMF delivery callback — runs on an RNS daemon thread."""
        try:
            if not self._running:
                return
            source = _source_hash_of(message)
            if getattr(message, "signature_validated", False):
                self._process_inbound(message, source)
                return
            self._quarantine_unverified(message, source,
                                        getattr(message, "unverified_reason", "no signature proof"))
        except Exception:
            logger.exception("Reticulum: error in LXMF delivery callback")

    def _process_inbound(self, message, source: Optional[str]) -> None:
        if not source:
            logger.debug("Reticulum: dropping LXMF message with no source hash")
            return

        if not self._is_allowed(source):
            logger.info("Reticulum: ignoring message from unauthorized %s", _short(source))
            if self.deny_reply and self._loop is not None and not self._loop.is_closed():
                self._loop.call_soon_threadsafe(
                    self._schedule_denial, source)
            return

        raw_hash = getattr(message, "hash", None)
        message_id = bytes(raw_hash).hex() if isinstance(raw_hash, (bytes, bytearray)) else None
        if message_id and self._is_duplicate(source, message_id):
            logger.info("Reticulum: dropping duplicate delivery of %s from %s",
                        message_id[:12], _short(source))
            return

        text_parts: list[str] = []
        try:
            title = (message.title_as_string() or "").strip()
        except Exception:
            title = ""
        try:
            content = (message.content_as_string() or "").strip()
        except Exception:
            content = ""
        if title:
            text_parts.append(title)
        if content:
            text_parts.append(content)

        # Voice notes: FIELD_AUDIO (Sideband PTT) and audio file attachments.
        # Saved into the audio cache so the gateway's own STT pipeline
        # transcribes them exactly like a Telegram/WhatsApp voice message.
        audio_paths: list[str] = []
        audio_mimes: list[str] = []
        audio_names: list[str] = []
        try:
            audio_paths, audio_mimes, audio_names, audio_notes = _extract_audio_fields(
                message, self.max_audio_bytes)
        except Exception:
            logger.exception("Reticulum: could not extract audio from message")
            audio_notes = []
        if audio_notes:
            text_parts.extend(audio_notes)
        if audio_paths:
            logger.info("Reticulum: voice note from %s (%d clip(s), %d bytes total)",
                        _short(source), len(audio_paths),
                        sum(os.path.getsize(path) for path in audio_paths if os.path.exists(path)))

        text = "\n".join(part for part in text_parts if part).strip()
        if not text and not audio_paths:
            logger.debug("Reticulum: dropping empty message from %s", _short(source))
            return

        try:
            timestamp = float(getattr(message, "timestamp", None) or time.time())
        except (TypeError, ValueError):
            timestamp = time.time()
        received_at = time.time()
        # Keep the sender's own timestamp even when it is old: a message that
        # spent a week in store-and-forward is still that old, and rewriting it
        # to "now" destroys the ordering the stamp exists to preserve. Only
        # non-finite values and clocks running far ahead are corrected, and the
        # latter cannot be trusted as a "late arrival" baseline.
        if not math.isfinite(timestamp) or timestamp <= 0:
            timestamp = received_at
        elif timestamp > received_at + 7 * 86400:
            logger.info("Reticulum: correcting a future-dated timestamp from %s", _short(source))
            timestamp = received_at

        meta = {
            "lxmf_source": source,
            "signature_validated": True,
            "method": getattr(message, "method", None),
            "rssi": getattr(message, "rssi", None),
            "snr": getattr(message, "snr", None),
            "sent_at": timestamp,
            "received_at": received_at,
            "media_urls": audio_paths,
            "media_types": audio_mimes,
            "media_filenames": audio_names,
        }
        if message_id:
            meta["lxmf_message_id"] = message_id
        record = {
            "source": source,
            "text": text,
            "timestamp": timestamp,
            "received_at": received_at,
            "meta": meta,
            "message_id": message_id,
        }
        if self._loop is not None and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._submit_inbound, record)

    def _quarantine_unverified(self, message, source: Optional[str], reason) -> None:
        """Hold an unverified message and retry once its source identity is known.

        LXMF only checks signatures against identities already in the local
        recall cache, so a legitimate first message from an allow-listed peer
        can arrive before its announce does. Instead of dropping it outright,
        request the path and re-unpack the raw envelope once the identity
        lands. Allow-list membership is required before anything is retained.
        """
        packed = getattr(message, "packed", None)
        if not packed or not source or not self._is_allowed(source):
            logger.warning(
                "Reticulum: dropping unverified LXMF message from %s (%s)",
                _short(source), reason)
            return
        logger.info(
            "Reticulum: quarantining unverified message from %s (%s); awaiting identity",
            _short(source), reason)
        try:
            import RNS
            with contextlib.suppress(Exception):
                RNS.Transport.request_path(bytes.fromhex(source))
        except Exception:
            logger.debug("Reticulum: path request for %s failed", _short(source))
        if self._loop is not None and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(
                self._schedule_quarantine_retry, bytes(packed), source)

    def _schedule_quarantine_retry(self, packed: bytes, source: str) -> None:
        if self._loop is not None and not self._loop.is_closed():
            self._loop.create_task(self._retry_unverified(packed, source, 45.0))

    async def _retry_unverified(self, packed: bytes, source: str, budget: float = 45.0) -> None:
        import RNS
        import LXMF
        try:
            source_hash = bytes.fromhex(source)
        except ValueError:
            return
        deadline = time.monotonic() + budget
        resolved = False
        while time.monotonic() < deadline:
            if not self._running:
                return
            if RNS.Identity.recall(source_hash) is not None:
                resolved = True
                break
            with contextlib.suppress(Exception):
                RNS.Transport.request_path(source_hash)
            await asyncio.sleep(2.0)
        if not resolved:
            logger.info(
                "Reticulum: dropping quarantined message from %s (identity never resolved)",
                _short(source))
            return
        try:
            message = LXMF.LXMessage.unpack_from_bytes(packed)
        except Exception as exc:
            logger.warning("Reticulum: re-unpack of quarantined message failed: %s", exc)
            return
        if not getattr(message, "signature_validated", False):
            logger.warning(
                "Reticulum: quarantined message from %s still unverified after identity recall; dropping",
                _short(source))
            return
        logger.info("Reticulum: recovered quarantined message from %s", _short(source))
        self._process_inbound(message, source)

    # -- inbound sequencing --------------------------------------------------

    def _is_duplicate(self, source: str, message_id: str) -> bool:
        """True when this exact LXMF envelope was already accepted.

        LXMF may hand the same message to the delivery callback twice (a DIRECT
        link proof plus a store-and-forward copy), which would otherwise run
        the same user turn twice.
        """
        now = time.time()
        key = (source, message_id.lower())
        seen = self._seen_messages
        if key in seen and now - seen[key] <= self._seen_messages_ttl:
            return True
        seen[key] = now
        seen.move_to_end(key)
        while seen and (len(seen) > 2048
                        or now - next(iter(seen.values())) > self._seen_messages_ttl):
            seen.popitem(last=False)
        return False

    def _submit_inbound(self, record: dict) -> None:
        """Queue an inbound record on the event loop and reorder within the window.

        Mesh links deliver out of order: a retry over a slower path can land
        after a newer message. Hold each source's arrivals for a short quiet
        period, sort by the sender's own LXMF timestamp, then dispatch in that
        order. Anything that still arrives late (past the window) is dispatched
        immediately and flagged, because a turn cannot be rewound.
        """
        if self._loop is None or self._loop.is_closed():
            return
        self._order_seq += 1
        record["seq"] = self._order_seq
        if record.get("bypass_reorder") or self.reorder_window <= 0:
            self._flush_records([record], source=record["source"])
            return
        source = record["source"]
        pending = self._order_pending.setdefault(source, [])
        pending.append(record)
        if len(pending) >= self.reorder_max_batch:
            self._flush_pending(source)
            return
        prior = self._order_tasks.get(source)
        if prior is not None and not prior.done():
            prior.cancel()
        self._order_tasks[source] = self._loop.create_task(self._flush_after_window(source))

    async def _flush_after_window(self, source: str) -> None:
        try:
            await asyncio.sleep(self.reorder_window)
        except asyncio.CancelledError:
            return
        self._flush_pending(source)

    def _flush_pending(self, source: str) -> None:
        records = self._order_pending.pop(source, [])
        task = self._order_tasks.pop(source, None)
        if task is not None and not task.done() and task is not asyncio.current_task():
            task.cancel()
        if records:
            self._flush_records(records, source=source)

    def _flush_records(self, records: list[dict[str, Any]], *, source: str) -> None:
        records = sorted(records, key=lambda item: (item["timestamp"], item["seq"]))
        arrival_order = [item["seq"] for item in sorted(records, key=lambda item: item["seq"])]
        sent_order = [item["seq"] for item in records]
        if arrival_order != sent_order:
            logger.info("Reticulum: reordered %d message(s) from %s by their timestamps",
                        len(records), _short(source))
        last = self._last_sent_at.get(source)
        for item in records:
            # Only messages that arrive after the window closed can be late;
            # within the window the sort already restored the true order. A
            # sender whose clock runs ahead must not poison every later message.
            item["out_of_order"] = bool(
                last is not None
                and item["timestamp"] < last - 0.5
                and item["timestamp"] < item["received_at"] - 0.5)
            if item["out_of_order"]:
                logger.info(
                    "Reticulum: out-of-order message from %s (sent %s, %.0fs before the last one handled)",
                    _short(source), _utc_stamp(item["timestamp"]),
                    max(0.0, last - item["timestamp"]))
        newest = max(item["timestamp"] for item in records)
        baseline = min(newest, max(item["received_at"] for item in records) + 2.0)
        self._last_sent_at[source] = max(last or 0.0, baseline)
        if self._loop is None or self._loop.is_closed():
            return
        self._loop.create_task(self._dispatch_in_order(records))

    async def _dispatch_in_order(self, records: list[dict[str, Any]]) -> None:
        for item in records:
            if not self._running:
                return
            try:
                await self._dispatch_message(
                    item["text"], item["source"], item["timestamp"], item["meta"],
                    received_at=item["received_at"],
                    out_of_order=item.get("out_of_order", False))
            except Exception:
                logger.exception("Reticulum: failed to dispatch inbound message from %s",
                                 _short(item["source"]))

    def _schedule_denial(self, source: str) -> None:
        now = time.time()
        last = self._denial_times.get(source, 0.0)
        if now - last < 300.0:
            return
        if len(self._denial_times) > 256:
            self._denial_times = {
                key: ts for key, ts in self._denial_times.items() if now - ts < 300.0}
        self._denial_times[source] = now
        if self._loop is not None and not self._loop.is_closed():
            self._loop.create_task(self._send_denial(source))

    async def _dispatch_message(self, text: str, source_hash: str,
                                timestamp: float, meta: dict,
                                *, received_at: Optional[float] = None,
                                out_of_order: bool = False) -> None:
        if not meta.get("call") and not meta.get("media_urls") and _is_call_request(text):
            # Consume exact chat commands here, before the LLM can turn them
            # into a voice note, a promise, or an accidental second call.
            if time.time() - timestamp > 120.0:
                await self.send(source_hash, "That call request is old. Send 'call me' again when ready.")
                return
            try:
                result = await asyncio.to_thread(self.call_peer, source_hash)
            except Exception as exc:
                logger.exception("Reticulum: call request failed")
                result = {"success": False, "error": str(exc)}
            if not result.get("success"):
                await self.send(source_hash, "Cannot call: " + result.get("error", "dialing failed"))
            return
        peer_name = self._stack.peer_names.get(source_hash) if self._stack else None
        short = f"lxmf:{_short(source_hash)}"
        session_source = self.build_source(
            chat_id=source_hash,
            chat_name=peer_name or short,
            chat_type="dm",
            user_id=source_hash,
            user_name=peer_name or source_hash,
        )
        message_id = meta.get("lxmf_message_id") or f"{source_hash}:{int(timestamp)}"
        media_urls = list(meta.get("media_urls") or [])
        media_types = list(meta.get("media_types") or [])
        media_names = list(meta.get("media_filenames") or [])
        received_at = received_at or time.time()

        # The sender's own LXMF timestamp leads every message body. The agent
        # reads it directly, so a late arrival still slots into the right place
        # in the conversation and can be acknowledged as late.
        body = text
        if self.timestamps:
            stamp = _inbound_stamp(timestamp, received_at, out_of_order)
            body = f"{stamp}{text}" if text else stamp.strip()
        elif out_of_order:
            body = f"[out of order] {text}" if text else "[out of order: voice message]"

        is_voice = bool(media_urls) and any(mime.startswith("audio/") for mime in media_types)
        # No ``metadata`` here on purpose: native admission only whitelists
        # WhatsApp's own keys and refuses any other non-empty metadata dict
        # (gateway/session_envelope.py:_native_metadata). The timing facts live
        # in raw_message instead, which is not part of the admission check.
        event = MessageEvent(
            text=body,
            message_type=MessageType.VOICE if is_voice else MessageType.TEXT,
            user_id=source_hash,
            user_name=peer_name or source_hash,
            source=session_source,
            message_id=message_id,
            timestamp=datetime.fromtimestamp(timestamp, tz=UTC),
            media_urls=media_urls,
            media_types=media_types,
            media_filenames=media_names,
            raw_message=meta,
        )
        logger.debug("Reticulum: message from %s sent %s%s: %.80s",
                     _short(source_hash), _utc_stamp(timestamp),
                     " (late)" if out_of_order else "", text)
        if meta.get("call_live"):
            # A live phone turn is latency-sensitive: skip the 1.5s text
            # batching window and hand it straight to the session runner.
            await self.handle_message(event)
        else:
            self._enqueue_text_event(event)

    def _text_batch_key(self, event: MessageEvent) -> str:
        return f"reticulum:{event.source.chat_id}"

    async def _send_denial(self, source_hash: str) -> None:
        if not self._is_allowed(source_hash):
            with contextlib.suppress(Exception):
                await self.send(source_hash, "Access not authorized.")

    # -- outbound ------------------------------------------------------------

    def call_peer(self, chat_id: str) -> dict:
        """Dial an authorized LXMF peer's telephone using its public identity."""
        target = str(chat_id or "").strip()
        if not _is_destination_hash(target):
            return {"success": False, "error": "Expected a 32-character LXMF destination hash."}
        target = target.lower()
        if not self._is_allowed(target):
            return {"success": False, "error": "Call destination is not authorized."}
        bridge = self._telephone
        if bridge is None:
            return {"success": False, "error": "Telephone service is not running."}
        if bridge.active_call():
            return {"success": False, "error": "Telephone line is busy."}
        identity = self._resolve_identity(bytes.fromhex(target))
        if identity is None:
            return {"success": False, "error": "Peer identity is unknown. Open Sideband and announce."}
        import RNS
        if RNS.Destination.hash(identity, "lxmf", "delivery").hex() != target:
            return {"success": False, "error": "Recalled identity does not match the LXMF destination."}
        return bridge.dial(identity, reply_to=target)

    def _resolve_identity(self, destination_hash: bytes):
        """Recall a peer identity, requesting a path and waiting if needed."""
        import RNS
        identity = RNS.Identity.recall(destination_hash)
        if identity is not None:
            return identity
        with contextlib.suppress(Exception):
            RNS.Transport.request_path(destination_hash)
        deadline = time.time() + 15.0
        while time.time() < deadline:
            time.sleep(0.25)
            identity = RNS.Identity.recall(destination_hash)
            if identity is not None:
                return identity
        return None

    def _wait_for_path(self, destination_hash: bytes, timeout: float) -> bool:
        import RNS
        if RNS.Transport.has_path(destination_hash):
            return True
        with contextlib.suppress(Exception):
            RNS.Transport.request_path(destination_hash)
        deadline = time.time() + timeout
        while time.time() < deadline:
            if RNS.Transport.has_path(destination_hash):
                return True
            time.sleep(0.25)
        return RNS.Transport.has_path(destination_hash)

    async def send(self, chat_id: str, content: str,
                   reply_to: Optional[str] = None,
                   metadata: Optional[dict[str, Any]] = None) -> SendResult:
        if not self._running or self._stack is None:
            return SendResult(success=False, error="Reticulum platform is not connected")

        destination = _normalize_hash(chat_id)
        if not _is_destination_hash(destination):
            return SendResult(
                success=False,
                error=f"Invalid LXMF destination hash {chat_id!r} (expected 32 hex characters)")

        text, media_count = _strip_media_tags(content)
        text = _CONTROL_RE.sub("", _ANSI_RE.sub("", text))
        if media_count and not text:
            text = "[attachment not delivered: send audio as a voice note instead]"
        text = _strip_markdown(text).strip()
        # While a telephone call is up, the answer belongs in the caller's ear
        # rather than in an LXMF chat that is not open. Speak it through the
        # live LXST transmit mixer and keep the normal text path only as a
        # fallback when synthesis is unavailable.
        if text and self._telephone is not None and self._telephone.has_active_call(destination):
            spoken = await asyncio.to_thread(self._telephone.speak, destination, text)
            if spoken:
                logger.info("Reticulum: reply spoken into live call with %s", _short(destination))
                return SendResult(success=True, message_id=f"call:{int(time.time())}")
            logger.warning("Reticulum: in-call TTS unavailable for %s; falling back to LXMF text",
                           _short(destination))
        # Reserve room for the timestamp prefix so stamped parts still fit.
        budget = max(200, self.max_message_length - (40 if self.timestamps else 0))
        parts = _split_message(text, budget)
        if not parts:
            return SendResult(success=False, error="Refusing to send an empty LXMF message")
        if self.timestamps:
            sent_at = time.time()
            total = len(parts)
            parts = [
                f"{_outbound_stamp(sent_at, index, total)}{_strip_part_tag(part)}"
                for index, part in enumerate(parts, 1)
            ]

        last_id = None
        for index, part in enumerate(parts):
            ok, reason, message_id = await self._send_payload(
                destination, part,
                reply_to=reply_to if index == 0 else None)
            if not ok:
                return SendResult(success=False, error=f"LXMF delivery failed: {reason}")
            last_id = message_id or last_id
        return SendResult(success=True, message_id=last_id)

    async def _send_payload(self, destination_hex: str, text: str, *,
                            fields: Optional[dict[Any, Any]] = None,
                            reply_to: Optional[str] = None
                            ) -> tuple[bool, str, Optional[str]]:
        """Send one LXMF payload DIRECT, falling back to store-and-forward."""
        import RNS
        import LXMF

        destination_hash = bytes.fromhex(destination_hex)
        identity = await asyncio.to_thread(self._resolve_identity, destination_hash)
        if identity is None:
            return False, (
                "unknown LXMF destination — the peer has not announced or no path was found"), None

        dest = RNS.Destination(
            identity, RNS.Destination.OUT, RNS.Destination.SINGLE, *_LXMF_DELIVERY_ASPECT)
        if bytes(dest.hash) != destination_hash:
            return False, "recalled identity does not match the destination hash", None

        if not await asyncio.to_thread(self._wait_for_path, destination_hash, 15.0):
            logger.info("Reticulum: no path to %s; trying DIRECT anyway", _short(destination_hex))

        ok, reason, message_id = await self._send_with_method(
            dest, text, LXMF.LXMessage.DIRECT, self.direct_timeout,
            fields=fields, reply_to=reply_to)
        if ok:
            return True, reason, message_id
        logger.info("Reticulum: DIRECT to %s failed (%s)", _short(destination_hex), reason)

        if not self.propagated_fallback:
            return False, reason, message_id

        node = self._stack.router.get_outbound_propagation_node() if self._stack else None
        if node is None:
            return False, f"{reason}; no propagation node configured for fallback", message_id
        if not await asyncio.to_thread(self._wait_for_path, node, 15.0):
            return False, f"{reason}; no path to propagation node {_short(node.hex())}", message_id

        logger.info("Reticulum: sending to %s via propagation node %s",
                    _short(destination_hex), _short(node.hex()))
        return await self._send_with_method(
            dest, text, LXMF.LXMessage.PROPAGATED, self.propagated_timeout,
            fields=fields, reply_to=reply_to)

    async def _send_with_method(self, dest, text: str, method, timeout: float, *,
                                fields: Optional[dict[Any, Any]] = None,
                                reply_to: Optional[str] = None
                                ) -> tuple[bool, str, Optional[str]]:
        import LXMF

        loop = asyncio.get_running_loop()
        done = asyncio.Event()
        outcome: dict[str, Any] = {"ok": False, "reason": "timeout", "id": None}
        message_fields = dict(fields or {})
        # Thread the reply anchor so the phone client can show the answer
        # against the message it answers (LXMF FIELD_REPLY_TO).
        if reply_to and _is_message_hash(reply_to):
            message_fields.setdefault(
                getattr(LXMF, "FIELD_REPLY_TO", 0x30), bytes.fromhex(reply_to.strip().lower()))

        def _finish(ok: bool, lxm) -> None:
            outcome["ok"] = ok
            outcome["reason"] = f"{'delivered' if ok else 'failed'} ({_state_name(lxm)})"
            with contextlib.suppress(Exception):
                outcome["id"] = lxm.hash.hex() if lxm.hash else None
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(done.set)

        def _on_delivered(lxm) -> None:
            _finish(True, lxm)

        def _on_failed(lxm) -> None:
            _finish(False, lxm)

        try:
            lxm = LXMF.LXMessage(
                dest,
                self._stack.destination,
                text,
                desired_method=method,
                fields=message_fields or None,
                include_ticket=True,
            )
            lxm.register_delivery_callback(_on_delivered)
            lxm.register_failed_callback(_on_failed)
        except Exception as exc:
            return False, f"could not construct LXMF message: {exc}", None

        threading.Thread(
            target=self._outbound_worker, args=(lxm,),
            name="lxmf-outbound", daemon=True).start()

        label = "PROPAGATED" if method == LXMF.LXMessage.PROPAGATED else "DIRECT"
        started = time.monotonic()
        logger.info("Reticulum: %s send to %s (%d bytes%s); waiting up to %.0fs",
                    label, _short(dest.hash.hex()), len(text.encode("utf-8")),
                    f", {len(message_fields)} field(s)" if message_fields else "", timeout)
        try:
            await asyncio.wait_for(done.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            # Log first: the fail_message call below has wedged before, and a
            # silent timeout leaves callers thinking the send is still live.
            logger.info("Reticulum: %s send to %s timed out after %.0fs; marking failed",
                        label, _short(dest.hash.hex()), timeout)
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(self._stack.router.fail_message, lxm), timeout=10)
            except Exception as exc:
                logger.debug("Reticulum: fail_message after timeout raised: %s", exc)
            return False, f"timeout after {timeout:.0f}s", outcome["id"]
        logger.info("Reticulum: %s send to %s %s in %.1fs",
                    label, _short(dest.hash.hex()),
                    "delivered" if outcome["ok"] else outcome["reason"],
                    time.monotonic() - started)
        return outcome["ok"], outcome["reason"], outcome["id"]

    # -- voice out ------------------------------------------------------------

    def _prepare_voice_payload(self, audio_path: str) -> tuple[Optional[bytes], str]:
        """``(ogg_opus_bytes, error)`` for an outbound voice note.

        Always Ogg/Opus: that is what Sideband and the other LXMF phone clients
        play natively (AM_OPUS_OGG). The clip is capped at ``tts_max_bytes`` so
        a long spoken reply cannot turn into a multi-minute LoRa transfer.
        """
        path = os.path.abspath(os.path.expanduser(str(audio_path or "")))
        if not path or not os.path.isfile(path):
            return None, f"audio file not found: {audio_path!r}"
        if os.path.getsize(path) <= 0:
            return None, "audio file is empty"
        ogg_path = None
        try:
            ogg_path = _transcode_audio_file(path, self.audio_bitrate)
            if ogg_path is None:
                if Path(path).suffix.lower() in {".ogg", ".opus"}:
                    ogg_path = path  # no ffmpeg: assume the caller supplied Opus
                else:
                    return None, "ffmpeg is not available to encode the voice note as Ogg/Opus"
            size = os.path.getsize(ogg_path)
            if size > self.tts_max_bytes:
                # One retry at a lower bitrate before giving up on audio.
                lean = _transcode_audio_file(path, "12k")
                if lean and os.path.getsize(lean) <= self.tts_max_bytes:
                    with contextlib.suppress(OSError):
                        os.unlink(ogg_path)
                    ogg_path, size = lean, os.path.getsize(lean)
                else:
                    if lean:
                        with contextlib.suppress(OSError):
                            os.unlink(lean)
                    return None, (
                        f"voice note is {size} bytes, over the {self.tts_max_bytes} byte "
                        "mesh budget; raise RETICULUM_TTS_MAX_BYTES to allow it")
            with open(ogg_path, "rb") as handle:
                return handle.read(), ""
        except Exception as exc:
            return None, f"could not encode voice note: {exc}"
        finally:
            if ogg_path and ogg_path != path:
                with contextlib.suppress(OSError):
                    os.unlink(ogg_path)

    async def send_voice(self, chat_id: str, audio_path: str, caption: Optional[str] = None,
                         reply_to: Optional[str] = None,
                         metadata: Optional[dict[str, Any]] = None,
                         **kwargs: Any) -> SendResult:
        """Send a native LXMF voice note (FIELD_AUDIO, Ogg/Opus).

        This is the TTS reply path: ``/voice on`` in a chat, or ``voice.auto_tts``
        in config.yaml, makes the gateway synthesize spoken replies for voice
        input and hand the file here.
        """
        if not self._running or self._stack is None:
            return SendResult(success=False, error="Reticulum platform is not connected")
        destination = _normalize_hash(chat_id)
        if not _is_destination_hash(destination):
            return SendResult(
                success=False,
                error=f"Invalid LXMF destination hash {chat_id!r} (expected 32 hex characters)")

        # A tool-generated voice clip or an auto-TTS file played during a call
        # must go to the handset, not into the LXMF queue.
        if self._telephone is not None and self._telephone.has_active_call(destination):
            spoken = await asyncio.to_thread(
                self._telephone.speak, destination, caption, audio_path)
            if spoken:
                logger.info("Reticulum: queued voice clip into live call with %s",
                            _short(destination))
                return SendResult(success=True, message_id=f"call:{int(time.time())}")

        audio, error = await asyncio.to_thread(self._prepare_voice_payload, audio_path)
        if audio is None:
            logger.warning("Reticulum: voice note to %s not sent: %s",
                           _short(destination), error)
            return SendResult(success=False, error=error)

        body = _strip_markdown(_CONTROL_RE.sub("", caption or "")).strip()
        if not body:
            body = "[voice message]"
        if self.timestamps:
            body = f"{_outbound_stamp(time.time())}{body}"
        import LXMF
        ok, reason, message_id = await self._send_payload(
            destination, body,
            fields={getattr(LXMF, "FIELD_AUDIO", 0x07): [AUDIO_MODE_OPUS_OGG, audio]},
            reply_to=reply_to)
        if not ok:
            logger.warning("Reticulum: voice note to %s failed: %s", _short(destination), reason)
            return SendResult(success=False, error=f"LXMF voice delivery failed: {reason}")
        logger.info("Reticulum: voice note (%d bytes) delivered to %s in %s",
                    len(audio), _short(destination), reason)
        return SendResult(success=True, message_id=message_id)

    def _outbound_worker(self, lxm) -> None:
        """Run ``handle_outbound`` off the event loop (it can block on the TX lock)."""
        try:
            self._stack.router.handle_outbound(lxm)
        except Exception as exc:
            logger.warning("Reticulum: handle_outbound failed: %s", exc)
            with contextlib.suppress(Exception):
                self._stack.router.fail_message(lxm)

    # -- adapter surface -----------------------------------------------------

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """LXMF has no typing indicator."""

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        destination = _normalize_hash(chat_id)
        name = None
        if self._stack is not None:
            name = self._stack.peer_names.get(destination)
        return {
            "chat_id": destination,
            "type": "dm",
            "name": name or f"lxmf:{_short(destination)}",
        }

    async def list_channels(self) -> Optional[list[dict[str, Any]]]:
        """Known allow-listed peers plus every announce-derived display name."""
        if self._stack is None:
            return None
        entries: list[dict[str, Any]] = []
        seen: set[str] = set()
        for destination in sorted(self._allowed_users()):
            if not _is_destination_hash(destination) or destination in seen:
                continue
            seen.add(destination)
            entries.append({
                "id": destination,
                "name": self._stack.peer_names.get(destination, f"lxmf:{_short(destination)}"),
                "type": "dm",
            })
        for destination, name in sorted(self._stack.peer_names.items()):
            if destination in seen:
                continue
            seen.add(destination)
            entries.append({"id": destination, "name": name, "type": "dm"})
        return entries


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------

def check_requirements() -> bool:
    """Passive dependency probe — never installs anything."""
    return _deps_available()


def validate_config(config) -> bool:
    """Configured when the storage path is usable (identity is auto-generated)."""
    extra = getattr(config, "extra", {}) or {}
    storage = _env_or_extra("RETICULUM_STORAGE", extra, "storage_path", DEFAULT_STORAGE_PATH)
    try:
        Path(os.path.expanduser(str(storage))).mkdir(parents=True, exist_ok=True)
        return True
    except OSError as exc:
        logger.warning("Reticulum: storage path %s is not usable: %s", storage, exc)
        return False


def is_connected(config) -> bool:
    """Configured for status displays: dependencies present and storage writable."""
    return _deps_available() and validate_config(config)


def _env_enablement() -> Optional[dict]:
    """Seed ``PlatformConfig.extra`` from the profile's environment."""
    seed = seed_extra_from_env((
        ("RETICULUM_DISPLAY_NAME", "display_name", None),
        ("RETICULUM_STORAGE", "storage_path", None),
        ("RETICULUM_CONFIG", "rns_config", None),
        ("RETICULUM_STAMP_COST", "stamp_cost", int),
        ("RETICULUM_ANNOUNCE_INTERVAL", "announce_interval", float),
        ("RETICULUM_PROPAGATION_NODE", "outbound_propagation_node", None),
        ("RETICULUM_ALLOWED_USERS", "allowed_users",
         lambda value: [part.strip() for part in value.split(",") if part.strip()]),
        ("RETICULUM_ALLOW_ALL_USERS", "allow_all_users", _truthy),
        ("RETICULUM_DENY_REPLY", "deny_reply", _truthy),
        ("RETICULUM_PROPAGATED_FALLBACK", "propagated_fallback", _truthy),
        ("RETICULUM_TIMESTAMPS", "timestamps", _truthy),
        ("RETICULUM_REORDER_WINDOW", "reorder_window", float),
        ("RETICULUM_AUDIO_BITRATE", "audio_bitrate", str),
        ("RETICULUM_TTS_MAX_BYTES", "tts_max_bytes", int),
        ("RETICULUM_MAX_AUDIO_BYTES", "max_audio_bytes", int),
        ("RETICULUM_TELEPHONE", "telephone", _truthy),
        ("RETICULUM_CALL_MAX_SECONDS", "call_max_seconds", int),
        ("RETICULUM_CALL_GREETING", "call_greeting", None),
    ), home_env="RETICULUM_HOME_CHANNEL")
    if not seed and not _truthy(platform_gate_env("RETICULUM_ENABLED")):
        return None
    if not seed:
        seed = {"enabled": True}
    return seed


async def _standalone_send(
    pconfig, chat_id: str, message: str, *,
    thread_id: Optional[str] = None, media_files: Optional[list[str]] = None,
    force_document: bool = False,
) -> dict[str, Any]:
    """Out-of-process sender for cron when no gateway is co-resident.

    Opens the profile's LXMF stack, sends DIRECT, and waits for delivery
    proof. Media is not supported (text body only).
    """
    if not _deps_available():
        return send_error("RNS/LXMF not installed. Run: pip install rns lxmf")
    extra = getattr(pconfig, "extra", {}) or {}
    storage = os.path.expanduser(str(
        _env_or_extra("RETICULUM_STORAGE", extra, "storage_path", DEFAULT_STORAGE_PATH)))
    rns_config = _env_or_extra("RETICULUM_CONFIG", extra, "rns_config", None)
    rns_config = os.path.expanduser(str(rns_config)) if rns_config else None
    display_name = str(_env_or_extra(
        "RETICULUM_DISPLAY_NAME", extra, "display_name", DEFAULT_DISPLAY_NAME))
    destination = _normalize_hash(chat_id)
    if not _is_destination_hash(destination):
        return send_error(f"Invalid LXMF destination hash {chat_id!r} (expected 32 hex characters)")
    text, _ = _strip_media_tags(message)
    text = _strip_markdown(text).strip()
    if not text:
        return send_error("Refusing to send an empty LXMF message")
    if _truthy(_env_or_extra("RETICULUM_TIMESTAMPS", extra, "timestamps",
                             DEFAULT_TIMESTAMPS), DEFAULT_TIMESTAMPS):
        text = f"{_outbound_stamp(time.time())}{text}"

    timeout = max(10.0, _as_float(
        _env_or_extra("RETICULUM_DELIVERY_TIMEOUT", extra, "delivery_timeout",
                      DEFAULT_DELIVERY_TIMEOUT), DEFAULT_DELIVERY_TIMEOUT))

    try:
        # RNS installs signal handlers, so build the stack on the calling
        # (main) thread, not inside the worker below.
        stack = _get_stack(
            storage_path=storage, rns_config=rns_config,
            display_name=display_name,
            stamp_cost=_as_int(_env_or_extra("RETICULUM_STAMP_COST", extra, "stamp_cost",
                                             DEFAULT_STAMP_COST), DEFAULT_STAMP_COST))
    except Exception as exc:
        return send_error(f"Reticulum initialisation failed: {exc}")

    def _send() -> dict[str, Any]:
        import RNS
        import LXMF

        identity = RNS.Identity.recall(bytes.fromhex(destination))
        if identity is None:
            RNS.Transport.request_path(bytes.fromhex(destination))
            deadline = time.time() + 15.0
            while time.time() < deadline and identity is None:
                time.sleep(0.25)
                identity = RNS.Identity.recall(bytes.fromhex(destination))
        if identity is None:
            return send_error("Unknown LXMF destination — the peer has not announced")

        dest = RNS.Destination(
            identity, RNS.Destination.OUT, RNS.Destination.SINGLE, *_LXMF_DELIVERY_ASPECT)
        finished = threading.Event()
        result: dict[str, Any] = {"ok": False, "reason": "timeout", "id": None}

        def _done(ok: bool, lxm) -> None:
            result.update(ok=ok, reason=_state_name(lxm),
                          id=lxm.hash.hex() if lxm.hash else None)
            finished.set()

        lxm = LXMF.LXMessage(
            dest, stack.destination, text,
            desired_method=LXMF.LXMessage.DIRECT, include_ticket=True)
        lxm.register_delivery_callback(lambda msg: _done(True, msg))
        lxm.register_failed_callback(lambda msg: _done(False, msg))
        stack.router.handle_outbound(lxm)
        if not finished.wait(timeout):
            return send_error(f"LXMF delivery timed out after {timeout:.0f}s")
        if not result["ok"]:
            return send_error(f"LXMF delivery failed: {result['reason']}")
        return {"success": True, "platform": "reticulum", "chat_id": destination,
                "message_id": result["id"]}

    try:
        return await asyncio.to_thread(_send)
    except Exception as exc:
        return send_error(f"Reticulum send failed: {exc}")


_SETUP_PROMPTS = (
    ("RETICULUM_DISPLAY_NAME", "Display name announced on the mesh (default: Hermes)"),
    ("RETICULUM_STORAGE", "LXMF storage directory (default: ~/.lxmf/storage)"),
    ("RETICULUM_ALLOWED_USERS", "Allowed LXMF hashes (comma-separated; blank = deny all)"),
    ("RETICULUM_PROPAGATION_NODE", "Outbound propagation node hash (optional)"),
    ("RETICULUM_HOME_CHANNEL", "Home channel LXMF hash for cron/notifications (optional)"),
)


def interactive_setup() -> None:
    """``hermes setup gateway`` wizard."""
    from hermes_cli.setup import (
        get_env_value, print_header, print_info, prompt, save_env_value,
    )
    from hermes_cli.setup_platforms import declines_reconfigure

    print_header("Reticulum (LXMF)")
    if declines_reconfigure("Reticulum", "Reconfigure Reticulum?", "RETICULUM_DISPLAY_NAME"):
        return
    for line in (
        "Requirements:",
        "  1. A Reticulum config at ~/.reticulum (or set RETICULUM_CONFIG).",
        "  2. Python packages rns and lxmf installed in the Hermes runtime.",
        "  3. Your mesh peers must allow-list this node's LXMF address.",
    ):
        print_info(line)
    for var, question in _SETUP_PROMPTS:
        suffix = " [keep current]" if get_env_value(var) else ""
        value = prompt(f"{question}{suffix}")
        if value:
            save_env_value(var, value)
    print_info("Done. Enable the plugin and restart the gateway:")
    print_info("  hermes plugins enable reticulum-platform")
    print_info("  hermes gateway restart")


_STAMP_TOOL_SCHEMA = {
    "name": "reticulum_stamp_cost",
    "description": (
        "Get or change the LXMF inbound proof-of-work stamp cost for the "
        "Reticulum adapter. cost=0 disables stamps; any other value requires "
        "that many leading zero bits from senders. Changes apply immediately "
        "(router enforcement + announce) and persist to RETICULUM_STAMP_COST "
        "in the Hermes .env unless persist=false."),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["get", "set"],
                "description": "Read current state (get) or change it (set). Default: get.",
            },
            "cost": {
                "type": "integer",
                "minimum": 0,
                "maximum": 99,
                "description": "New inbound stamp cost (LXMF cap is 100, so 99 max). 0 disables stamps.",
            },
            "persist": {
                "type": "boolean",
                "description": "Save the new cost to the Hermes .env so it survives restarts (default: true).",
            },
        },
    },
}


def _stamp_tool(args: dict, **_: Any) -> str:
    """Tool handler: read or change the adapter's inbound stamp requirement."""
    action = str(args.get("action") or "get").strip().lower()
    if action not in ("get", "set"):
        return "Error: action must be 'get' or 'set'."
    adapter = _LIVE_ADAPTER
    if action == "get" or args.get("cost") is None:
        if adapter is None:
            return json.dumps({"connected": False,
                               "note": "reticulum adapter is not running"})
        return json.dumps(adapter.stamp_status())
    if adapter is None:
        return "Error: reticulum adapter is not connected; start the gateway first."
    try:
        status = adapter.set_stamp_cost(int(args["cost"]))
    except Exception as exc:
        return f"Error: {exc}"
    if bool(args.get("persist", True)):
        try:
            from hermes_cli.setup import save_env_value
            value = str(status.get("stamp_cost"))
            save_env_value("RETICULUM_STAMP_COST", value)
            os.environ["RETICULUM_STAMP_COST"] = value
            status["persisted"] = True
        except Exception as exc:
            status["persisted"] = False
            status["persist_error"] = str(exc)
    return json.dumps(status)


_NAME_TOOL_SCHEMA = {
    "name": "reticulum_display_name",
    "description": (
        "Get or change the display name the Reticulum adapter announces on "
        "the mesh (shows up in Sideband and other LXMF clients). Changes apply "
        "immediately with a re-announce and persist to RETICULUM_DISPLAY_NAME "
        "in the Hermes .env unless persist=false."),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["get", "set"],
                "description": "Read the current name (get) or change it (set). Default: get.",
            },
            "name": {
                "type": "string",
                "description": "New display name (1-64 UTF-8 bytes).",
            },
            "persist": {
                "type": "boolean",
                "description": "Save the new name to the Hermes .env so it survives restarts (default: true).",
            },
        },
    },
}


def _name_tool(args: dict, **_: Any) -> str:
    """Tool handler: read or change the announced display name."""
    action = str(args.get("action") or "get").strip().lower()
    if action not in ("get", "set"):
        return "Error: action must be 'get' or 'set'."
    adapter = _LIVE_ADAPTER
    if action == "get" or not args.get("name"):
        if adapter is None:
            return json.dumps({"connected": False,
                               "note": "reticulum adapter is not running"})
        return json.dumps(adapter.name_status())
    if adapter is None:
        return "Error: reticulum adapter is not connected; start the gateway first."
    try:
        status = adapter.set_display_name(str(args["name"]))
    except Exception as exc:
        return f"Error: {exc}"
    if bool(args.get("persist", True)):
        try:
            from hermes_cli.setup import save_env_value
            save_env_value("RETICULUM_DISPLAY_NAME", str(status.get("display_name", "")))
            os.environ["RETICULUM_DISPLAY_NAME"] = str(status.get("display_name", ""))
            status["persisted"] = True
        except Exception as exc:
            status["persisted"] = False
            status["persist_error"] = str(exc)
    return json.dumps(status)


_CALL_TOOL_SCHEMA = {
    "name": "reticulum_call",
    "description": (
        "Place an outgoing Reticulum/LXST voice call when the user asks 'call me', "
        "'give me a call', or similar. With no destination, call the current "
        "Reticulum chat sender. Only use this for an immediate call the user "
        "asked for now, never for a scheduled or conditional request. Do not "
        "substitute a voice note for an explicit call request. The destination, "
        "if supplied, must be an allow-listed LXMF delivery hash, not a phone "
        "number or telephone hash. Returns dialing, not proof of answer."),
    "parameters": {
        "type": "object",
        "properties": {
            "destination": {"type": "string", "description":
                            "Optional LXMF delivery hash; omit in a Reticulum chat to call its sender."},
        },
    },
}


def _call_tool(args: dict, **_: Any) -> str:
    target = args.get("destination")
    if not target:
        from gateway.session_context import get_session_env
        if get_session_env("HERMES_SESSION_PLATFORM", "") != "reticulum":
            return json.dumps({"success": False, "error":
                               "Specify an LXMF destination, or ask from your Reticulum chat."})
        target = get_session_env("HERMES_SESSION_CHAT_ID", "")
    adapter = _LIVE_ADAPTER
    if adapter is None:
        return json.dumps({"success": False, "error": "Reticulum adapter is not connected."})
    return json.dumps(adapter.call_peer(str(target)))


def register(ctx) -> None:
    """Hermes plugin entry point."""
    ctx.register_tool(
        name="reticulum_call", toolset="reticulum", schema=_CALL_TOOL_SCHEMA,
        handler=_call_tool, description="Call an authorized LXMF peer's handset on request",
        emoji="📞",
    )
    ctx.register_tool(
        name="reticulum_stamp_cost",
        toolset="reticulum",
        schema=_STAMP_TOOL_SCHEMA,
        handler=_stamp_tool,
        description="Get or change the LXMF inbound stamp requirement at runtime",
        emoji="\U0001f4e1",
    )
    ctx.register_tool(
        name="reticulum_display_name",
        toolset="reticulum",
        schema=_NAME_TOOL_SCHEMA,
        handler=_name_tool,
        description="Get or change the announced LXMF display name at runtime",
        emoji="\U0001f4e1",
    )
    ctx.register_platform(
        name="reticulum",
        label="Reticulum (LXMF)",
        adapter_factory=lambda cfg: ReticulumAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        install_hint="pip install rns lxmf   # Reticulum + LXMF Python packages",
        setup_fn=interactive_setup,
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="RETICULUM_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="RETICULUM_ALLOWED_USERS",
        allow_all_env="RETICULUM_ALLOW_ALL_USERS",
        max_message_length=MAX_MESSAGE_LENGTH,
        emoji="📡",
        pii_safe=True,
        allow_update_command=False,
        platform_hint=(
            "You are chatting over Reticulum/LXMF, an off-grid-capable mesh "
            "network. Clients include Sideband, Nomad Network and custom LXMF "
            "apps. Messages are plain text; markdown is stripped. Every message "
            "you send is prefixed with a UTC send timestamp, and every incoming "
            "message carries the sender's own timestamp — when one is marked "
            "'out of order' it was sent before a message you already answered, "
            "so place it at the right point in the conversation instead of "
            "treating it as the newest. Voice notes arrive as audio and are "
            "transcribed automatically; with voice replies enabled (/voice on) "
            "your answer is also sent as an Ogg/Opus voice note. You can make "
            "outgoing LXST voice calls with reticulum_call when the user requests "
            "an immediate call; omit destination to call this chat sender. Never "
            "use it for a scheduled or conditional call. Do not substitute "
            "a voice note for an explicitly requested phone call. Bandwidth may be "
            "very limited (LoRa), delivery can be slow, and the peer may be "
            "offline (replies fall back to store-and-forward when a propagation "
            "node is configured). Keep answers short and self-contained; avoid "
            "large code blocks and attachments."),
    )
