"""LXST telephone bridge for the Reticulum platform.

Sideband (and other LXST clients) dial an ``lxst.telephony`` destination derived
from the same identity as the LXMF delivery address. This module gives Hermes
such an endpoint: it announces it, answers allow-listed callers, keeps a full
recording for diagnostics, and runs a live conversation while the call is up.

Inbound audio is segmented on pauses and each completed utterance is
transcribed and handed to the agent through ``on_utterance``. The agent's
answer is delivered back into the active call as synthesized speech through a
queue-backed LXST source; no LXMF round trip is needed while the caller is on
the line. A call still produces a recording under ``<storage>/calls``.

The host is headless, so LXST's device constructors are replaced with
file/queue-backed objects for the lifetime of the bridge. Both are normal LXST
source and sink primitives, not forks of the telephone implementation.
"""

from __future__ import annotations

# pyright: reportMissingImports=false
import asyncio
import contextlib
import json
import logging
import os
import queue
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Callable, Optional

try:  # package import (normal plugin load)
    from .call_audio import (
        DEFAULT_SAMPLE_RATE,
        CallAudioBuffer,
        UtteranceSegmenter,
        decode_audio_to_pcm,
        encode_pcm_to_ogg,
        make_call_audio_source,
        make_call_recorder,
    )
except ImportError:  # pragma: no cover - loaded as a top-level module
    from call_audio import (  # type: ignore[no-redef]
        DEFAULT_SAMPLE_RATE,
        CallAudioBuffer,
        UtteranceSegmenter,
        decode_audio_to_pcm,
        encode_pcm_to_ogg,
        make_call_audio_source,
        make_call_recorder,
    )

logger = logging.getLogger(__name__)

DEFAULT_MAX_CALL_SECONDS = 300
DEFAULT_AUTO_ANSWER = 1.0
MAX_SPOKEN_REPLY_CHARS = 1800
LIVE_QUEUE_DEPTH = 6
VOICE_SERVICE_TIMEOUT = 10.0


def _voice_service_url(primary_env: str) -> str:
    """Return the configured warm voice-service URL, if any.

    ``RETICULUM_VOICE_URL`` is a shared convenience setting; the
    STT/TTS-specific variables win when both are present.
    """
    for key in (primary_env, "RETICULUM_VOICE_URL"):
        value = os.environ.get(key, "").strip().rstrip("/")
        if value:
            return value
    return ""


class TelephoneBridge:
    """Headless LXST telephone endpoint with in-call STT and spoken replies."""

    CALL_PROFILE_NAMES = {
        "ultra_low": "BANDWIDTH_ULTRA_LOW",
        "very_low": "BANDWIDTH_VERY_LOW",
        "low": "BANDWIDTH_LOW",
        "medium": "QUALITY_MEDIUM",
        "high": "QUALITY_HIGH",
        "max": "QUALITY_MAX",
        "latency_low": "LATENCY_LOW",
        "latency_ultra_low": "LATENCY_ULTRA_LOW",
    }

    def __init__(
        self,
        identity,
        *,
        storage_path: str,
        allowed: Callable[[str], bool],
        on_transcript: Callable[[dict], None],
        on_utterance: Optional[Callable[[dict], None]] = None,
        greeting_path: Optional[str] = None,
        max_call_seconds: float = DEFAULT_MAX_CALL_SECONDS,
        auto_answer: float = DEFAULT_AUTO_ANSWER,
        live_transcription: bool = True,
    ):
        self.identity = identity
        self.storage_path = storage_path
        self.allowed = allowed
        self.on_transcript = on_transcript
        self.on_utterance = on_utterance if live_transcription else None
        self.greeting_path = greeting_path
        self.max_call_seconds = max(30.0, float(max_call_seconds or DEFAULT_MAX_CALL_SECONDS))
        self.auto_answer = auto_answer
        self.address: Optional[str] = None

        self._telephone = None
        self._telephony_module = None
        self._original_line_sink = None
        self._original_line_source = None
        self._lock = threading.Lock()
        self._dial_lock = threading.Lock()
        self._call: Optional[dict] = None
        self._sink = None
        self._outbound: Optional[CallAudioBuffer] = None
        self._speech_generation = 0
        self._audio_source = None
        self._greeting_pcm = None
        self._greeting_loaded = False
        self._watchdog: Optional[threading.Thread] = None
        self._running = False

        # Utterances are written and transcribed on one worker so two short
        # clauses cannot race each other into the agent out of order.
        self._live_queue: queue.Queue = queue.Queue(maxsize=LIVE_QUEUE_DEPTH)
        self._live_worker: Optional[threading.Thread] = None
        self._live_stop = threading.Event()
        self._utterance_seq = 0

    # -- lifecycle -----------------------------------------------------------

    def start(self) -> bool:
        """Create the telephone destination. False when lxst is absent."""
        if self._running:
            return True
        try:
            import LXST  # noqa: F401
            from LXST.Primitives import Telephony as telephony
        except Exception as exc:
            logger.info(
                "Reticulum: lxst is not installed (%s); telephone calls disabled. "
                "Install with: pip install lxst", exc)
            return False

        self._telephony_module = telephony
        if not self.greeting_path:
            self.greeting_path = self._speech_greeting()
        try:
            self._install_headless_audio()
            telephone = self._telephone_class(telephony)(
                self.identity,
                auto_answer=self.auto_answer,
                allowed=self._caller_allowed,
            )
            telephone.set_ringing_callback(self._on_ringing)
            telephone.set_established_callback(self._on_established)
            telephone.set_ended_callback(self._on_ended)
        except Exception as exc:
            logger.warning("Reticulum: could not start the LXST telephone: %s", exc)
            self._restore_audio_hooks()
            return False

        self._telephone = telephone
        self.address = bytes(telephone.destination.hash).hex()
        self._running = True
        if self.on_utterance is not None:
            self._start_live_worker()
        # No announce here: RNS interfaces are still negotiating seconds after
        # startup, and an announce sent now reaches nobody while also tripping
        # the per-destination announce limiter. The adapter's announce loop
        # does the first announce after its warm-up and repeats every interval.
        logger.info(
            "Reticulum: telephone ready at %s (LXST, live transcription %s, "
            "max %ds, calls to %s)",
            self.address, "on" if self.on_utterance else "off",
            int(self.max_call_seconds), self._calls_dir())
        return True

    def announce(self) -> None:
        telephone = self._telephone
        if telephone is None or not self._running:
            return
        try:
            telephone.announce()
        except Exception as exc:
            logger.debug("Reticulum: telephone re-announce failed: %s", exc)

    def stop(self) -> None:
        self._running = False
        self._live_stop.set()
        telephone = self._telephone
        self._telephone = None
        if telephone is not None:
            with contextlib.suppress(Exception):
                telephone.teardown()
        self._finish_recording(reason="bridge stopped")
        self._restore_audio_hooks()

    def dial(self, identity, *, reply_to: str, timeout: float = 20.0) -> dict:
        """Dial a known peer, waiting only for route discovery, not an answer."""
        import RNS
        if not self._dial_lock.acquire(blocking=False):
            return {"success": False, "error": "Another call is already being dialed."}
        try:
            telephone = self._telephone
            if not self._running or telephone is None:
                return {"success": False, "error": "Telephone service is not running."}
            with self._lock:
                if self._call or telephone.active_call or telephone.busy:
                    return {"success": False, "error": "Telephone line is busy."}
            caller = bytes(identity.hash).hex()
            destination = RNS.Destination.hash(identity, "lxst", "telephony")
            deadline = time.monotonic() + max(0.0, min(float(timeout), 30.0))
            if not RNS.Transport.has_path(destination):
                RNS.Transport.request_path(destination)
            while not RNS.Transport.has_path(destination):
                if not self._running or time.monotonic() >= deadline:
                    return {"success": False,
                            "error": "No telephone route. Open Sideband and announce, then retry."}
                time.sleep(0.2)
            # Reserve before call(): RNS callbacks may run immediately. Never
            # replace an incoming call that arrived during path discovery.
            with self._lock:
                if self._call or telephone.active_call or telephone.busy:
                    return {"success": False, "error": "Telephone line is busy."}
                self._call = self._new_call(caller, reply_to, direction="outgoing")
                token = self._call
                self._outbound = CallAudioBuffer()
                self._speech_generation += 1
                self._greeting_loaded = False
                self._utterance_seq = 0
            module = self._telephony_module
            if module is None:
                raise RuntimeError("Telephone module is not loaded.")
            profiles = module.Profiles
            # LXST's 5 s link-establishment timeout is too short on a mesh
            # path; the 70 s path/ring wait still bounds the call.
            if hasattr(telephone, "establishment_timeout"):
                telephone.establishment_timeout = max(
                    float(telephone.establishment_timeout or 0), 45.0)
            profile_name = os.environ.get("RETICULUM_CALL_PROFILE", "medium").strip().lower()
            profile = getattr(profiles, self.CALL_PROFILE_NAMES.get(profile_name, "QUALITY_MEDIUM"))
            # Reserve the line against a simultaneous incoming call while
            # LXST creates the outgoing link. The status is CALLING after
            # call() returns, so clearing the external flag reopens nothing.
            telephone.set_busy(True)
            try:
                telephone.call(identity, profile=profile, mode=profiles.MODE_FULL_DUPLEX)
            finally:
                telephone.set_busy(False)
            with self._lock:
                started = (self._call is token and telephone.active_call is not None
                           and bool(getattr(telephone.active_call, "is_outgoing", False)))
            if not started:
                if self._call is token:
                    self._finish_recording(reason="dial did not start")
                elif getattr(telephone.active_call, "is_outgoing", False):
                    # An incoming call won the race after LXST created our
                    # link; do not leave an orphaned outgoing link alive.
                    with contextlib.suppress(Exception):
                        telephone.hangup()
                return {"success": False, "error": "Could not start the telephone call."}
            threading.Thread(target=self._watch_outgoing, args=(token,),
                             name="lxst-dial-watch", daemon=True).start()
            logger.info("Reticulum: dialing telephone %s for LXMF peer %s",
                        destination.hex(), reply_to)
            return {"success": True, "status": "dialing",
                    "telephone": destination.hex(), "reply_to": reply_to}
        except Exception as exc:
            logger.exception("Reticulum: outbound telephone call failed")
            if self._call and self._call.get("direction") == "outgoing":
                if self._telephone is not None and self._telephone.active_call:
                    with contextlib.suppress(Exception):
                        self._telephone.hangup()
                else:
                    self._finish_recording(reason="dial failed")
            return {"success": False, "error": str(exc)}
        finally:
            self._dial_lock.release()

    def _watch_outgoing(self, token: dict) -> None:
        deadline = time.monotonic() + 75.0
        while self._running:
            with self._lock:
                if self._call is not token or token.get("answered_at") is not None:
                    return
            telephone = self._telephone
            if telephone is None or telephone.active_call is None:
                self._finish_recording(reason="outgoing call did not connect")
                return
            if time.monotonic() >= deadline:
                with contextlib.suppress(Exception):
                    telephone.hangup()
                return
            time.sleep(0.5)

    def _new_call(self, caller: str, reply_to: str, *, direction: str) -> dict:
        os.makedirs(self._calls_dir(), exist_ok=True)
        path = os.path.join(
            self._calls_dir(),
            f"call_{caller[:12]}_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.ogg")
        return {"caller": caller, "reply_to": reply_to, "path": path,
                "started_at": time.time(), "answered_at": None,
                "direction": direction, "utterances": 0, "live_delivered": 0}

    # -- headless audio plumbing --------------------------------------------

    def _install_headless_audio(self) -> None:
        """Replace LXST's device constructors with queue-backed equivalents."""
        module = self._telephony_module
        assert module is not None
        self._original_line_sink = getattr(module, "LineSink", None)
        self._original_line_source = getattr(module, "LineSource", None)
        bridge = self

        def _sink(preferred_device=None, low_latency=False, **kwargs):
            return bridge._make_sink()

        def _source(preferred_device=None, target_frame_ms=None, codec=None,
                    sink=None, filters=None, skip=None, ease_in=None, **kwargs):
            return bridge._make_source(
                target_frame_ms=target_frame_ms, codec=codec, sink=sink)

        module.LineSink = _sink
        module.LineSource = _source

    def _restore_audio_hooks(self) -> None:
        module = self._telephony_module
        if module is None:
            return
        if self._original_line_sink is not None:
            module.LineSink = self._original_line_sink
        if self._original_line_source is not None:
            module.LineSource = self._original_line_source

    def _make_sink(self):
        with self._lock:
            path = (self._call or {}).get("path") or self._scratch_path()
        segmenter = None
        if self.on_utterance is not None:
            segmenter = UtteranceSegmenter(
                self._queue_utterance,
                on_speech_start=self._interrupt_playback,
            )
        recorder = make_call_recorder(path, segmenter)
        original_handle = recorder.handle_frame

        def receive(frame, source=None):
            with self._lock:
                call = self._call
                if (call and call.get("direction") == "outgoing"
                        and call.get("answered_at") is None):
                    return  # Local ringing tones are not caller speech.
            original_handle(frame, source)

        recorder.handle_frame = receive
        self._sink = recorder
        return recorder

    def _make_source(self, *, target_frame_ms=None, codec=None, sink=None):
        frame_ms = int(target_frame_ms or 60)
        if self._outbound is None:
            self._outbound = CallAudioBuffer()
        with self._lock:
            call = self._call
            greeting_ready = not (call and call.get("direction") == "outgoing"
                                  and call.get("answered_at") is None)
        if greeting_ready:
            self._load_greeting_into(self._outbound)
        source = make_call_audio_source(
            target_frame_ms=frame_ms, codec=codec, sink=sink,
            buffer=self._outbound)
        self._audio_source = source
        return source

    def _load_greeting_into(self, buffer: CallAudioBuffer) -> None:
        if self._greeting_loaded:
            return
        self._greeting_loaded = True
        if not self.greeting_path:
            return
        path = os.path.expanduser(str(self.greeting_path))
        if not os.path.isfile(path):
            logger.warning("Reticulum: call greeting %s not found", path)
            return
        self._greeting_pcm = decode_audio_to_pcm(path, DEFAULT_SAMPLE_RATE)
        if self._greeting_pcm is not None and self._greeting_pcm.size:
            buffer.set_greeting(self._greeting_pcm)
            logger.info("Reticulum: call greeting loaded from %s (%.1fs)",
                        path, self._greeting_pcm.shape[0] / DEFAULT_SAMPLE_RATE)

    def _speech_greeting(self) -> Optional[str]:
        """Render a short spoken greeting, cached on disk.

        The warm GPU service is preferred when configured; edge-tts remains
        the fallback for hosts that do not run the local voice backend.
        """
        text = os.environ.get("RETICULUM_CALL_GREETING_TEXT",
                              "Hermes here. Go ahead.").strip()
        voice = os.environ.get("RETICULUM_CALL_GREETING_VOICE",
                               "en-US-AriaNeural").strip()
        os.makedirs(self._calls_dir(), exist_ok=True)
        local_path = os.path.join(self._calls_dir(), "greeting-speech.wav")
        if os.path.isfile(local_path) and os.path.getsize(local_path) > 0:
            return local_path
        if self._render_local_tts(text, local_path):
            logger.info("Reticulum: call greeting spoken by the GPU voice service: %r", text)
            return local_path
        path = os.path.join(self._calls_dir(), "greeting-speech.mp3")
        if os.path.isfile(path) and os.path.getsize(path) > 0:
            return path
        if self._render_edge_tts(text, path, voice):
            logger.info("Reticulum: call greeting spoken by %s: %r", voice, text)
            return path
        if self._render_edge_tts_cli(text, path, voice):
            logger.info("Reticulum: call greeting spoken by %s: %r", voice, text)
            return path
        logger.warning("Reticulum: could not synthesize the call greeting")
        return None

    @staticmethod
    def _render_local_tts(text: str, path: str,
                          timeout: float = VOICE_SERVICE_TIMEOUT) -> bool:
        """Render speech through the warm localhost GPU voice service."""
        url = _voice_service_url("RETICULUM_CALL_TTS_URL")
        if not url:
            return False
        try:
            request = urllib.request.Request(
                f"{url}/tts",
                data=json.dumps({"text": text}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=timeout) as response:
                audio = response.read()
            if not audio:
                logger.warning("Reticulum: GPU voice service returned empty TTS audio")
                return False
            Path(path).write_bytes(audio)
            return True
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            logger.warning("Reticulum: GPU TTS unavailable (%s); falling back", exc)
            return False

    @staticmethod
    def _render_edge_tts(text: str, path: str, voice: str,
                         timeout: float = 60.0) -> bool:
        """Use the gateway's importable edge_tts API, without relying on PATH.

        The gateway service runs with a PATH that does not include the plugin
        environment's bin directory. Importing the module is both available and
        more reliable than hunting for a console script.
        """
        try:
            import edge_tts
        except Exception:
            return False
        result = {"ok": False}

        def _worker():
            try:
                async def _render():
                    await edge_tts.Communicate(text, voice).save(path)
                asyncio.run(_render())
                result["ok"] = os.path.isfile(path) and os.path.getsize(path) > 0
            except Exception as exc:
                logger.warning("Reticulum: edge-tts render failed: %s", exc)

        worker = threading.Thread(target=_worker, name="lxst-edge-tts", daemon=True)
        worker.start()
        worker.join(timeout)
        if worker.is_alive():
            logger.warning("Reticulum: edge-tts render timed out after %.0fs", timeout)
            return False
        return bool(result["ok"])

    @staticmethod
    def _render_edge_tts_cli(text: str, path: str, voice: str,
                             timeout: float = 60.0) -> bool:
        """CLI fallback for hosts that install edge-tts without the module."""
        exe = TelephoneBridge._edge_tts_executable()
        if not exe:
            return False
        try:
            result = subprocess.run(
                [exe, "--text", text, "--voice", voice, "--write-media", path],
                capture_output=True, timeout=timeout, stdin=subprocess.DEVNULL, check=False)
            if result.returncode == 0 and os.path.isfile(path) and os.path.getsize(path) > 0:
                return True
            logger.warning("Reticulum: edge-tts CLI failed: %s",
                           (result.stderr or b"").decode("utf-8", "replace")[:200])
        except Exception as exc:
            logger.warning("Reticulum: edge-tts CLI failed: %s", exc)
        return False

    @staticmethod
    def _edge_tts_executable() -> Optional[str]:
        import sys
        candidate = os.path.join(os.path.dirname(sys.executable), "edge-tts")
        if os.path.isfile(candidate):
            return candidate
        return shutil.which("edge-tts")

    def _synthesize_reply(self, text: str) -> Optional[str]:
        """Synthesize an in-call reply, preferring the warm GPU voice service."""
        clean = " ".join(str(text or "").split())
        if not clean:
            return None
        if len(clean) > MAX_SPOKEN_REPLY_CHARS:
            clean = clean[:MAX_SPOKEN_REPLY_CHARS].rsplit(" ", 1)[0] + "."
        if _voice_service_url("RETICULUM_CALL_TTS_URL"):
            fd, local_path = tempfile.mkstemp(prefix="lxst_reply_", suffix=".wav")
            os.close(fd)
            if self._render_local_tts(clean, local_path):
                return local_path
            with contextlib.suppress(OSError):
                os.unlink(local_path)
        voice = os.environ.get(
            "RETICULUM_CALL_TTS_VOICE",
            os.environ.get("RETICULUM_CALL_GREETING_VOICE", "en-US-AriaNeural"),
        ).strip()
        fd, path = tempfile.mkstemp(prefix="lxst_reply_", suffix=".mp3")
        os.close(fd)
        if self._render_edge_tts(clean, path, voice):
            return path
        if self._render_edge_tts_cli(clean, path, voice):
            return path
        with contextlib.suppress(OSError):
            os.unlink(path)
        return None

    # Bandwidth caps for inbound calls. The caller proposes a profile and
    # LXST normally honours it; RETICULUM_CALL_PROFILE caps it so a mesh link
    # spends a few kbit/s instead of ~24 kbit/s on every call.

    def _profile_cap(self, telephony):
        name = os.environ.get("RETICULUM_CALL_PROFILE", "auto").strip().lower()
        if name in ("", "auto", "default", "caller"):
            return None
        attribute = self.CALL_PROFILE_NAMES.get(name)
        if attribute is None:
            logger.warning("Reticulum: unknown RETICULUM_CALL_PROFILE %r; using the caller's preference", name)
            return None
        return getattr(telephony.Profiles, attribute)

    def _telephone_class(self, telephony):
        cap = self._profile_cap(telephony)
        if cap is None:
            return telephony.Telephone
        profiles = telephony.Profiles
        logger.info("Reticulum: call profile capped at %s", profiles.profile_name(cap))

        class CappedTelephone(telephony.Telephone):
            def _clamp(self, profile):
                if profile is None:
                    return None
                try:
                    if profiles.profile_index(profile) > profiles.profile_index(cap):
                        logger.info("Reticulum: downgrading call profile %s to %s",
                                    profiles.profile_name(profile), profiles.profile_name(cap))
                        return cap
                except Exception:
                    pass
                return profile

            def switch_profile(self, profile=None, from_signalling=False):
                clamped = self._clamp(profile)
                result = super().switch_profile(clamped, from_signalling)
                # A remote-requested change suppresses LXST's normal echo.
                # If we capped it, tell the peer the actual negotiated value,
                # even when our profile already equals the cap.
                with contextlib.suppress(Exception):
                    if (from_signalling and profile is not None and clamped != profile
                            and self.call_status == telephony.Signalling.STATUS_ESTABLISHED
                            and self.active_call and self.active_call.profile == clamped):
                        self.signal(telephony.Signalling.PREFERRED_PROFILE + clamped,
                                    self.active_call)
                return result

            def _Telephone__select_call_profile(self, profile=None):
                clamped = self._clamp(profile if profile is not None
                                      else profiles.DEFAULT_PROFILE)
                result = super()._Telephone__select_call_profile(clamped)
                # The caller proposed the profile; tell it which one we settled
                # on so both ends encode and decode the same codec.
                with contextlib.suppress(Exception):
                    if self.active_call and getattr(self.active_call, "is_incoming", False):
                        self.signal(telephony.Signalling.PREFERRED_PROFILE + clamped,
                                    self.active_call)
                return result

        return CappedTelephone

    # -- call handling -------------------------------------------------------

    def _caller_allowed(self, identity_hash) -> bool:
        caller = bytes(identity_hash).hex()
        try:
            allowed = bool(self.allowed(caller))
        except Exception:
            logger.exception("Reticulum: caller allow-list check failed")
            allowed = False
        if not allowed:
            # Temporary bridge for clients that place calls from a second
            # identity: allow callers whose hash starts with an approved
            # prefix. Exact allow-list entries remain the primary control.
            prefixes = [p.strip().lower() for p in
                        os.environ.get("RETICULUM_CALL_ALLOW_PREFIXES", "").split(",")
                        if p.strip()]
            allowed = any(caller.startswith(prefix) for prefix in prefixes)
        logger.info("Reticulum: call from %s %s", caller,
                    "allowed" if allowed else "not on the allow-list")
        return allowed

    def _on_ringing(self, identity) -> None:
        caller = bytes(identity.hash).hex()
        # The call identity hash is NOT an LXMF destination: replies sent to it
        # fail with "unknown LXMF destination". Derive the peer's lxmf.delivery
        # address from the identity carried in the link request.
        reply_to = caller
        try:
            import RNS
            reply_to = RNS.Destination.hash(identity, "lxmf", "delivery").hex()
        except Exception:
            logger.debug("Reticulum: could not derive an LXMF address for %s",
                         caller, exc_info=True)
        with self._lock:
            self._call = self._new_call(caller, reply_to, direction="incoming")
            path = self._call["path"]
            sink = self._sink
        self._outbound = CallAudioBuffer()
        self._speech_generation = 0
        self._greeting_loaded = False
        self._utterance_seq = 0
        # The ringing sink is built before this callback runs, so it still has
        # the throwaway path; retarget it at the real recording file.
        if sink is not None and getattr(sink, "path", None) != path:
            with contextlib.suppress(Exception):
                sink.path = path
        logger.info("Reticulum: incoming call from %s — answering in %.1fs",
                    caller, self.auto_answer)

    def _on_established(self, identity) -> None:
        with self._lock:
            if not self._call:
                return
            self._call["answered_at"] = time.time()
            call = dict(self._call)
        if call.get("direction") == "outgoing" and self._outbound is not None:
            self._load_greeting_into(self._outbound)
        logger.info("Reticulum: call with %s established; recording to %s%s",
                    call["caller"], call["path"],
                    ", live transcription on" if self.on_utterance else "")
        telephone = self._telephone
        profile = getattr(getattr(telephone, "active_call", None), "profile", None)
        if profile is not None and self._telephony_module is not None:
            with contextlib.suppress(Exception):
                logger.info("Reticulum: call profile %s (%s)",
                            self._telephony_module.Profiles.profile_name(profile),
                            hex(profile))
        self._watchdog = threading.Thread(
            target=self._enforce_call_limit, args=(call["started_at"],),
            name="lxst-call-limit", daemon=True)
        self._watchdog.start()

    def _enforce_call_limit(self, started_at: float) -> None:
        deadline = started_at + self.max_call_seconds
        while time.time() < deadline:
            if not self._running or self._telephone is None:
                return
            with self._lock:
                if not self._call or self._call.get("started_at") != started_at:
                    return
            time.sleep(1.0)
        logger.info("Reticulum: call reached the %ds limit; hanging up",
                    int(self.max_call_seconds))
        telephone = self._telephone
        if telephone is not None:
            with contextlib.suppress(Exception):
                telephone.hangup()

    def _on_ended(self, identity) -> None:
        caller = bytes(identity.hash).hex() if identity is not None else None
        self._finish_recording(reason="call ended", caller_override=caller)

    def _finish_recording(self, *, reason: str,
                          caller_override: Optional[str] = None) -> None:
        # Finalize the sink while the call context is still visible: sink.stop()
        # flushes an in-flight utterance into the live STT queue.
        with self._lock:
            call = self._call
            sink = self._sink
            self._sink = None
        frames = int(getattr(sink, "frame_count", 0) or 0) if sink is not None else 0
        if sink is not None:
            try:
                sink.stop()  # flushes the final utterance and finalizes Ogg
            except Exception:
                logger.exception("Reticulum: could not finalize the call recording")
        # sink.stop() may have just queued the closing utterance. Give the live
        # worker a moment to transcribe it before deciding whether this call got
        # any live turn; otherwise a fast hangup looks like an STT failure.
        self._drain_live_queue(timeout=3.0)
        with self._lock:
            if self._call is call:
                self._call = None
            self._speech_generation += 1
            outbound = self._outbound
            self._outbound = None
        if outbound is not None:
            outbound.clear()
        if not call or call.get("answered_at") is None:
            return  # Never transcribe local ringing/busy tones as user speech.
        if caller_override:
            call["caller"] = caller_override
        call["ended_at"] = time.time()
        call["duration"] = max(0.0, call["ended_at"] - (call.get("answered_at") or call["started_at"]))
        call["reason"] = reason
        path = call.get("path")
        source = self._audio_source
        outbound_frames = (int(getattr(source, "frames_sent", 0) or 0)
                           if source is not None else 0)
        if not path or not os.path.isfile(path) or os.path.getsize(path) <= 0:
            logger.info(
                "Reticulum: call from %s produced no audio (%d frames captured, "
                "%d outbound frames)",
                call["caller"], frames, outbound_frames)
            return
        logger.info("Reticulum: call from %s recorded %.1fs (%d bytes, %d live utterances)",
                    call["caller"], call["duration"], os.path.getsize(path),
                    int(call.get("utterances") or 0))
        if source is not None:
            logger.info("Reticulum: in-call transmit source sent %d frames",
                        int(getattr(source, "frames_sent", 0) or 0))
        if self.on_utterance is not None:
            # Live turns already reached the agent one utterance at a time.
            # Keep the end-of-call callback for diagnostics, but mark it so an
            # adapter cannot dispatch the same conversation twice. If none of
            # the live utterances produced a turn (STT failed or was silent),
            # fall through to a full-call transcript instead of losing the call.
            if int(call.get("live_delivered") or 0) > 0:
                call["live"] = True
                call["transcript"] = ""
                call["error"] = ""
                self._deliver_transcript(call)
                return
            logger.info("Reticulum: no live utterances were delivered; "
                        "transcribing the call recording as a fallback")
        threading.Thread(target=self._transcribe_and_deliver, args=(call,),
                         name="lxst-call-transcribe", daemon=True).start()

    def _transcribe_and_deliver(self, call: dict) -> None:
        transcript = ""
        error = ""
        try:
            result = self._transcribe(call["path"])
            if result.get("success"):
                transcript = str(result.get("transcript") or "").strip()
            else:
                error = str(result.get("error") or "transcription failed")
        except Exception as exc:
            error = f"transcription failed: {exc}"
            logger.exception("Reticulum: call transcription raised")
        call["transcript"] = transcript
        call["error"] = error
        self._deliver_transcript(call)

    def _transcribe(self, path: str) -> dict:
        """Transcribe through the warm GPU service, falling back to Hermes STT."""
        url = _voice_service_url("RETICULUM_CALL_STT_URL")
        if url:
            language = (os.environ.get("RETICULUM_CALL_STT_LANGUAGE", "en").strip()
                        or None)
            try:
                timeout = float(os.environ.get("RETICULUM_CALL_STT_TIMEOUT", "30"))
                request = urllib.request.Request(
                    f"{url}/stt",
                    data=json.dumps({"path": os.path.abspath(path),
                                     "language": language}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                if payload.get("success"):
                    return {"success": True,
                            "transcript": str(payload.get("transcript") or ""),
                            "provider": "gpu"}
                logger.warning("Reticulum: GPU STT rejected %s: %s",
                               os.path.basename(path), payload.get("error"))
            except (urllib.error.URLError, TimeoutError, OSError,
                    ValueError, json.JSONDecodeError) as exc:
                logger.warning("Reticulum: GPU STT unavailable (%s); falling back", exc)
        from tools.transcription_tools import transcribe_audio
        return transcribe_audio(path, source="gateway")

    def _deliver_transcript(self, call: dict) -> None:
        try:
            self.on_transcript(call)
        except Exception:
            logger.exception("Reticulum: could not hand the call transcript to the adapter")

    # -- live conversation --------------------------------------------------

    def _start_live_worker(self) -> None:
        if self._live_worker is not None and self._live_worker.is_alive():
            return
        self._live_stop.clear()
        self._live_worker = threading.Thread(
            target=self._live_loop, name="lxst-live-stt", daemon=True)
        self._live_worker.start()

    def _queue_utterance(self, samples, sample_rate: int) -> None:
        with self._lock:
            if self._call is None:
                return
            self._call["utterances"] = int(self._call.get("utterances") or 0) + 1
            call = dict(self._call)
        if not self._running:
            return
        item = (call, samples, int(sample_rate or DEFAULT_SAMPLE_RATE))
        try:
            self._live_queue.put_nowait(item)
        except queue.Full:
            with contextlib.suppress(queue.Empty):
                self._live_queue.get_nowait()
            with contextlib.suppress(queue.Full):
                self._live_queue.put_nowait(item)
            logger.warning("Reticulum: live STT queue full; dropped the oldest utterance")
        logger.info("Reticulum: utterance from %s queued for live transcription (%.1fs)",
                    call["caller"], len(samples) / max(1, sample_rate))

    def _live_loop(self) -> None:
        while not self._live_stop.is_set():
            try:
                item = self._live_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                self._process_live_utterance(*item)
            except Exception:
                logger.exception("Reticulum: live utterance processing failed")
            finally:
                self._live_queue.task_done()

    def _drain_live_queue(self, timeout: float = 3.0) -> None:
        """Wait briefly for queued utterances to become live turns."""
        if self.on_utterance is None:
            return
        deadline = time.monotonic() + max(0.0, timeout)
        while time.monotonic() < deadline:
            with self._live_queue.mutex:
                pending = self._live_queue.unfinished_tasks
            if pending == 0:
                return
            time.sleep(0.02)
        logger.info("Reticulum: live STT queue still had work at call end")

    def _process_live_utterance(self, call: dict, samples, sample_rate: int) -> None:
        with self._lock:
            self._utterance_seq += 1
            seq = self._utterance_seq
        stem = Path(call["path"]).with_suffix("")
        utterance_path = os.path.join(
            str(stem) + "_utterances", f"utt_{seq:03d}.ogg")
        if not encode_utterance(utterance_path, samples, sample_rate):
            logger.warning("Reticulum: could not write live utterance %d for %s",
                           seq, call["caller"])
            return
        started = time.monotonic()
        result = self._transcribe(utterance_path)
        elapsed = time.monotonic() - started
        transcript = ""
        error = ""
        if result.get("success"):
            transcript = str(result.get("transcript") or "").strip()
        else:
            error = str(result.get("error") or "transcription failed")
        logger.info("Reticulum: live utterance %d from %s %.1fs -> STT in %.1fs: %r",
                    seq, call["caller"], len(samples) / max(1, sample_rate),
                    elapsed, transcript[:120])
        if not transcript:
            logger.info("Reticulum: live utterance %d had no transcript (%s)",
                        seq, error or "silence")
            return
        payload = dict(call)
        payload.update({
            "live": True,
            "utterance_index": seq,
            "utterance_path": utterance_path,
            "utterance_duration": len(samples) / max(1, sample_rate),
            "transcript": transcript,
            "error": error,
        })
        if self.on_utterance is not None:
            try:
                self.on_utterance(payload)
                with self._lock:
                    current = self._call
                    if current is not None and current.get("path") == call.get("path"):
                        current["live_delivered"] = int(
                            current.get("live_delivered") or 0) + 1
            except Exception:
                logger.exception("Reticulum: could not hand live utterance to the adapter")

    def _interrupt_playback(self) -> None:
        with self._lock:
            self._speech_generation += 1
            outbound = self._outbound
        if outbound is None:
            return
        pending = outbound.pending_samples
        if pending:
            outbound.clear()
            logger.info("Reticulum: caller started speaking; stopped %.1fs of queued reply audio",
                        pending / DEFAULT_SAMPLE_RATE)

    # -- outbound speech ----------------------------------------------------

    def has_active_call(self, reply_to: Optional[str] = None) -> bool:
        with self._lock:
            call = self._call
            if not call or call.get("answered_at") is None:
                return False
            if reply_to is None:
                return True
            target = str(reply_to or "").strip().lower()
            return target in (str(call.get("caller") or "").lower(),
                              str(call.get("reply_to") or "").lower())

    def active_call(self, reply_to: Optional[str] = None) -> Optional[dict]:
        with self._lock:
            if not self._call or not self.has_active_call_locked(reply_to):
                return None
            return dict(self._call)

    def has_active_call_locked(self, reply_to: Optional[str] = None) -> bool:
        call = self._call
        if not call or call.get("answered_at") is None:
            return False
        if reply_to is None:
            return True
        target = str(reply_to or "").strip().lower()
        return target in (str(call.get("caller") or "").lower(),
                          str(call.get("reply_to") or "").lower())

    def speak(self, reply_to: str, text: Optional[str] = None,
              audio_path: Optional[str] = None) -> bool:
        """Queue synthesized speech into the active call.

        ``audio_path`` is preferred when the gateway already synthesized the
        reply; otherwise ``text`` is rendered by the warm GPU voice service,
        with edge-tts as fallback. The method returns once audio is queued,
        not when playback finishes.
        """
        with self._lock:
            call = self._call
            if not call or not self.has_active_call_locked(reply_to):
                return False
            outbound = self._outbound
            generation = self._speech_generation
        if outbound is None:
            return False
        generated = None
        try:
            path = audio_path
            if not path:
                generated = self._synthesize_reply(text or "")
                path = generated
            if not path:
                return False
            samples = decode_audio_to_pcm(path, DEFAULT_SAMPLE_RATE)
            if samples is None or not samples.size:
                return False
            # Synthesis takes time; if the caller started a new utterance while
            # it ran, this reply is already stale and must not play over them.
            with self._lock:
                if (generation != self._speech_generation
                        or not self._call
                        or not self.has_active_call_locked(reply_to)):
                    logger.info("Reticulum: discarded a stale reply for %s",
                                str(call.get("caller") or "")[:12])
                    return False
                count = outbound.enqueue(samples)
            if count <= 0:
                return False
            logger.info("Reticulum: queued %.1fs of speech for call with %s",
                        count / DEFAULT_SAMPLE_RATE, call.get("caller"))
            return True
        finally:
            if generated:
                with contextlib.suppress(OSError):
                    os.unlink(generated)

    # -- helpers -------------------------------------------------------------

    def _calls_dir(self) -> str:
        return os.path.join(os.path.expanduser(self.storage_path), "calls")

    def _scratch_path(self) -> str:
        # A sink may be created at ringing, before the caller is known. Point it
        # at a throwaway name; _on_ringing retargets it at the real call file.
        os.makedirs(self._calls_dir(), exist_ok=True)
        return os.path.join(self._calls_dir(),
                            f"unused_{uuid.uuid4().hex[:8]}.ogg")


def encode_utterance(path: str, samples, sample_rate: int) -> bool:
    """Write a live utterance without importing the rest of the bridge."""
    return encode_pcm_to_ogg(path, samples, sample_rate)
