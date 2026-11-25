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
    RETICULUM_ANNOUNCE_INTERVAL  re-announce seconds        (default 900)
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
import threading
import time
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

DEFAULT_DISPLAY_NAME = "Hermes"
DEFAULT_STORAGE_PATH = "~/.lxmf/storage"
DEFAULT_STAMP_COST = 8
DEFAULT_ANNOUNCE_INTERVAL = 900.0
DEFAULT_DELIVERY_TIMEOUT = 150.0
DEFAULT_PROPAGATED_TIMEOUT = 180.0
DEFAULT_PROPAGATION_SYNC_INTERVAL = 60.0
DEFAULT_MAX_MESSAGE_LENGTH = 1000
DEFAULT_TEXT_BATCH_DELAY = 1.5
MIN_ANNOUNCE_INTERVAL = 300.0  # RNS rate-limits announces; below this is pointless
MAX_MESSAGE_LENGTH = 8000

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
    """Remove ``MEDIA:<path>`` directives; this bridge is text-only."""
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

        self._stack: Optional[_BridgeStack] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._announce_task: Optional[asyncio.Task] = None
        self._propagation_sync_task: Optional[asyncio.Task] = None
        self._running = False
        self._address_hex: Optional[str] = None
        self._denial_times: dict[str, float] = {}
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
            "allowed=%d propagation_node=%s fallback=%s",
            self.display_name, self.storage_path,
            self.stamp_cost if self.stamp_cost else "off",
            self._allow_all(), len(self._allowed_users()),
            self.propagation_node or ("auto" if self.auto_propagation_node else "none"),
            self.propagated_fallback)

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
        if self._stack is not None and self._stack.sink is self._sink_callback:
            self._stack.sink = None
        self._mark_disconnected()
        logger.info("Reticulum: adapter disconnected (RNS stack retained)")

    # -- announce loop -------------------------------------------------------

    async def _announce_loop(self) -> None:
        try:
            while self._running:
                await asyncio.sleep(self.announce_interval)
                if not self._running or self._stack is None:
                    continue
                try:
                    await asyncio.to_thread(self._stack.announce)
                    logger.debug("Reticulum: re-announced %s", self._address_hex)
                except Exception as exc:
                    logger.warning("Reticulum: re-announce failed: %s", exc)
        except asyncio.CancelledError:
            pass

    def _start_announce_loop(self) -> None:
        if self._loop is None or self._loop.is_closed():
            return
        if self._announce_task is not None and not self._announce_task.done():
            return
        self._announce_task = self._loop.create_task(self._announce_loop())

    def _stop_announce_loop(self) -> None:
        task, self._announce_task = self._announce_task, None
        if task is not None and not task.done():
            task.cancel()

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

        text = (message.content_as_string() or "").strip()
        try:
            title = (message.title_as_string() or "").strip()
        except Exception:
            title = ""
        if title:
            text = f"{title}\n{text}" if text else title
        if not text:
            logger.debug("Reticulum: dropping empty message from %s", _short(source))
            return

        if not self._is_allowed(source):
            logger.info("Reticulum: ignoring message from unauthorized %s", _short(source))
            if self.deny_reply and self._loop is not None and not self._loop.is_closed():
                self._loop.call_soon_threadsafe(
                    self._schedule_denial, source)
            return

        try:
            timestamp = float(getattr(message, "timestamp", None) or time.time())
        except (TypeError, ValueError):
            timestamp = time.time()
        if not math.isfinite(timestamp) or abs(time.time() - timestamp) > 30 * 86400:
            timestamp = time.time()
        meta = {
            "lxmf_source": source,
            "signature_validated": True,
            "method": getattr(message, "method", None),
            "rssi": getattr(message, "rssi", None),
            "snr": getattr(message, "snr", None),
        }
        raw_hash = getattr(message, "hash", None)
        if isinstance(raw_hash, (bytes, bytearray)):
            meta["lxmf_message_id"] = bytes(raw_hash).hex()
        if self._loop is not None and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(
                self._schedule_inbound, text, source, timestamp, meta)

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

    def _schedule_inbound(self, text: str, source: str, timestamp: float, meta: dict) -> None:
        if self._loop is not None and not self._loop.is_closed():
            self._loop.create_task(self._guarded_dispatch(text, source, timestamp, meta))

    async def _guarded_dispatch(self, text: str, source: str, timestamp: float, meta: dict) -> None:
        try:
            await self._dispatch_message(text, source, timestamp, meta)
        except Exception:
            logger.exception("Reticulum: failed to dispatch inbound message from %s", _short(source))

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
                                timestamp: float, meta: dict) -> None:
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
        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            user_id=source_hash,
            user_name=peer_name or source_hash,
            source=session_source,
            message_id=message_id,
            timestamp=datetime.fromtimestamp(timestamp, tz=UTC),
            raw_message=meta,
        )
        logger.debug("Reticulum: message from %s: %.80s", _short(source_hash), text)
        self._enqueue_text_event(event)

    def _text_batch_key(self, event: MessageEvent) -> str:
        return f"reticulum:{event.source.chat_id}"

    async def _send_denial(self, source_hash: str) -> None:
        if not self._is_allowed(source_hash):
            with contextlib.suppress(Exception):
                await self.send(source_hash, "Access not authorized.")

    # -- outbound ------------------------------------------------------------

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
            text = "[attachment not delivered: this LXMF bridge is text-only]"
        text = _strip_markdown(text).strip()
        parts = _split_message(text, self.max_message_length)
        if not parts:
            return SendResult(success=False, error="Refusing to send an empty LXMF message")

        last_id = None
        for part in parts:
            ok, reason, message_id = await self._send_text(destination, part)
            if not ok:
                return SendResult(success=False, error=f"LXMF delivery failed: {reason}")
            last_id = message_id or last_id
        return SendResult(success=True, message_id=last_id)

    async def _send_text(self, destination_hex: str, text: str) -> tuple[bool, str, Optional[str]]:
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
            dest, text, LXMF.LXMessage.DIRECT, self.direct_timeout)
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
            dest, text, LXMF.LXMessage.PROPAGATED, self.propagated_timeout)

    async def _send_with_method(self, dest, text: str, method, timeout: float
                                ) -> tuple[bool, str, Optional[str]]:
        import LXMF

        loop = asyncio.get_running_loop()
        done = asyncio.Event()
        outcome: dict[str, Any] = {"ok": False, "reason": "timeout", "id": None}

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
        logger.info("Reticulum: %s send to %s (%d bytes); waiting up to %.0fs",
                    label, _short(dest.hash.hex()), len(text.encode("utf-8")), timeout)
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


def register(ctx) -> None:
    """Hermes plugin entry point."""
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
            "apps. Messages are plain text; markdown is stripped. Bandwidth "
            "may be very limited (LoRa), delivery can be slow, and the peer "
            "may be offline (replies fall back to store-and-forward when a "
            "propagation node is configured). Keep answers short and "
            "self-contained; avoid large code blocks and attachments."),
    )
