"""LXST telephone bridge for the Reticulum platform.


Sideband (and other LXST clients) dial an ``lxst.telephony`` destination derived
from the same identity as the LXMF delivery address. This module gives Hermes
such an endpoint: it announces it, answers allow-listed callers, records the
call to Ogg/Opus, transcribes it with the gateway's own STT backend and hands
the transcript to the agent as an inbound message. The agent's answer then goes
back to the caller as an ordinary LXMF message (or a voice note when TTS is
enabled), so a "phone call" ends with a written answer in the caller's chat.

The host is headless, so LXST's default local audio devices are replaced with
file-backed objects: an OpusFileSink for the caller's audio and a short greeting
(or silence) for the transmitted side. Both are first-party LXST pipeline
primitives; only the two device constructors in
``LXST.Primitives.Telephony`` are swapped, and only for the lifetime of the
bridge.
"""

from __future__ import annotations

# pyright: reportMissingImports=false
import contextlib
import logging
import os
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger(__name__)

DEFAULT_MAX_CALL_SECONDS = 300
DEFAULT_AUTO_ANSWER = 1.0
GREETING_FILENAME = "telephone-greeting.ogg"
BEEP_SECONDS = 0.35
BEEP_FREQUENCY = 880

_ADAPTER_CLASS = None
_RECORDER_CLASS = None


def _recorder_class():
    """Build the recording Sink subclass lazily (LXST is optional)."""
    global _RECORDER_CLASS
    if _RECORDER_CLASS is not None:
        return _RECORDER_CLASS
    import wave

    import numpy as np
    from LXST.Sinks import Sink

    class _CallRecorder(Sink):
        """Collect decoded call frames and write them as Ogg/Opus.

        LXST's OpusFileSink assumes a live device pipeline and finalises
        asynchronously, which is fragile for answer-and-record use. This sink
        simply accumulates sample frames and transcodes once on stop.
        """

        def __init__(self, path):
            self.path = path
            self.samplerate = None
            self.channels = 1
            self.bitdepth = 32
            self.should_run = True
            self.buffer_max_height = 128
            self.autostart_min = 1
            self.streaming = True
            self.frame_count = 0
            self._chunks = []
            self._lock = threading.Lock()
            self._hw_frame_time = 0.0

        def can_receive(self, from_source=None):
            return True

        def handle_frame(self, frame, source=None):
            if isinstance(frame, (bytes, bytearray, memoryview)):
                frame = np.frombuffer(bytes(frame), dtype="<f4")
            samples = np.asarray(frame, dtype="float32")
            if samples.ndim == 1:
                samples = samples.reshape(-1, 1)
            with self._lock:
                if self.frame_count == 0:
                    logger.info("Reticulum: call audio frame %s from %s at %s Hz",
                                samples.shape, type(source).__name__,
                                getattr(source, "samplerate", None))
                self._chunks.append(samples)
                self.frame_count += 1
                if self.samplerate is None and source is not None:
                    self.samplerate = getattr(source, "samplerate", None) or 48000

        def wait_for_frames(self):
            return None

        def enable_low_latency(self):
            return None

        def start(self):
            return None

        def stop(self):
            with self._lock:
                chunks = self._chunks
                self._chunks = []
            if not chunks:
                return
            samples = np.concatenate(chunks, axis=0)
            samples = np.clip(samples, -1.0, 1.0)
            channels = samples.shape[1] if samples.ndim > 1 else 1
            samplerate = int(self.samplerate or 48000)
            pcm = (samples * 32767.0).astype("<i2")
            wav_path = self.path + ".wav"
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            with wave.open(wav_path, "wb") as handle:
                handle.setnchannels(channels)
                handle.setsampwidth(2)
                handle.setframerate(samplerate)
                handle.writeframes(pcm.tobytes())
            ffmpeg = shutil.which("ffmpeg")
            if not ffmpeg:
                return
            result = subprocess.run(
                [ffmpeg, "-v", "error", "-y", "-i", wav_path, "-ac", "1",
                 "-c:a", "libopus", "-b:a", "24k", "-application", "voip",
                 "-f", "ogg", self.path],
                capture_output=True, timeout=120, stdin=subprocess.DEVNULL, check=False)
            if result.returncode == 0:
                with contextlib.suppress(OSError):
                    os.unlink(wav_path)
            else:
                logger.warning("Reticulum: could not encode the call recording: %s",
                               (result.stderr or b"").decode("utf-8", "replace")[:200])

    _RECORDER_CLASS = _CallRecorder
    return _RECORDER_CLASS


def make_call_recorder(path: str):
    """A sink that records decoded call audio to ``path`` (Ogg/Opus)."""
    return _recorder_class()(path)



def _adapter_class():
    """Build the Sink subclass lazily: LXST is an optional dependency."""
    global _ADAPTER_CLASS
    if _ADAPTER_CLASS is not None:
        return _ADAPTER_CLASS
    from LXST.Sinks import Sink

    class _FileSinkAdapter(Sink):
        """Present an OpusFileSink to LXST's device-oriented receive pipeline.

        ``Telephony`` builds the receive pipeline with a ``Raw`` codec because
        ``LineSink`` decodes raw frames itself. ``OpusFileSink`` expects sample
        arrays, so unwrap the Raw envelope here and forward everything else.
        """

        def __init__(self, sink, codec):
            object.__setattr__(self, "_sink", sink)
            object.__setattr__(self, "_codec", codec)

        def __getattr__(self, name):
            return getattr(object.__getattribute__(self, "_sink"), name)

        def __setattr__(self, name, value):
            if name.startswith("_"):
                object.__setattr__(self, name, value)
            else:
                setattr(object.__getattribute__(self, "_sink"), name, value)

        def can_receive(self, from_source=None):
            return self._sink.can_receive(from_source)

        def handle_frame(self, frame, source=None):
            if isinstance(frame, (bytes, bytearray, memoryview)):
                frame = self._codec.decode(bytes(frame))
            if not getattr(self, "_logged", False):
                object.__setattr__(self, "_logged", True)
                logger.debug("Reticulum: call sink %s receiving frames from %s",
                             object.__getattribute__(self, "_sink"), source)
            return self._sink.handle_frame(frame, source)

    _ADAPTER_CLASS = _FileSinkAdapter
    return _ADAPTER_CLASS


def make_file_sink_adapter(sink):
    """Wrap an OpusFileSink so it can stand in for LineSink (also used by tests)."""
    from LXST.Codecs import Raw
    return _adapter_class()(sink, Raw())


class TelephoneBridge:
    """Headless LXST telephone endpoint: announce, answer, record, transcribe."""

    def __init__(self, identity, *, storage_path: str,
                 allowed: Callable[[str], bool],
                 on_transcript: Callable[[dict], None],
                 greeting_path: Optional[str] = None,
                 max_call_seconds: float = DEFAULT_MAX_CALL_SECONDS,
                 auto_answer: float = DEFAULT_AUTO_ANSWER):
        self.identity = identity
        self.storage_path = storage_path
        self.allowed = allowed
        self.on_transcript = on_transcript
        self.greeting_path = greeting_path
        self.max_call_seconds = max(30.0, float(max_call_seconds or DEFAULT_MAX_CALL_SECONDS))
        self.auto_answer = auto_answer
        self.address: Optional[str] = None
        self._telephone = None
        self._telephony_module = None
        self._original_line_sink = None
        self._original_line_source = None
        self._lock = threading.Lock()
        self._call: Optional[dict] = None
        self._sink = None
        self._watchdog: Optional[threading.Thread] = None
        self._running = False

    # -- lifecycle -----------------------------------------------------------

    def start(self) -> bool:
        """Create and announce the telephone destination. False when lxst is absent."""
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
        try:
            self._install_headless_audio()
            telephone = telephony.Telephone(
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
        try:
            telephone.announce()
        except Exception as exc:
            logger.warning("Reticulum: telephone announce failed: %s", exc)
        logger.info(
            "Reticulum: telephone ready at %s (LXST, calls from allow-listed peers; "
            "max %ds, recording to %s)",
            self.address, int(self.max_call_seconds), self._calls_dir())
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
        telephone = self._telephone
        self._telephone = None
        if telephone is not None:
            with contextlib.suppress(Exception):
                telephone.teardown()
        self._finish_recording(reason="bridge stopped")
        self._restore_audio_hooks()

    # -- headless audio plumbing --------------------------------------------

    def _install_headless_audio(self) -> None:
        """Replace LXST's device constructors with file-backed equivalents.

        ``Telephone`` builds ``LineSink``/``LineSource`` objects from its own
        module globals at ringing and call-setup time. On a headless host the
        sink constructor raises (no ALSA/Pulse device), which aborts call setup.
        """
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
        recorder = make_call_recorder(path)
        self._sink = recorder
        return recorder

    def _make_source(self, *, target_frame_ms=None, codec=None, sink=None):
        frame_ms = int(target_frame_ms or 60)
        greeting = self._greeting_for(frame_ms)
        if greeting and os.path.isfile(greeting):
            try:
                from LXST.Sources import OpusFileSource
                return OpusFileSource(greeting, target_frame_ms=frame_ms,
                                      codec=codec, sink=sink)
            except Exception as exc:
                logger.warning("Reticulum: could not open call greeting %s: %s", greeting, exc)
        from LXST.Generators import ToneSource
        # Silent carrier: keeps the transmit pipeline alive without a microphone.
        return ToneSource(frequency=440, gain=0.0, ease=False,
                          target_frame_ms=frame_ms, codec=codec, sink=sink)

    def _greeting_for(self, frame_ms: int) -> Optional[str]:
        """Greeting Ogg/Opus with frames of exactly ``frame_ms``.

        Opus only accepts frame durations from a fixed set, and the mixer
        rejects anything else, so the tone is rendered per negotiated frame
        size instead of reusing one file for every profile.
        """
        frame_ms = max(10, min(120, int(frame_ms or 60)))
        if frame_ms % 10:
            frame_ms = 60
        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg:
            return None
        source = None
        if self.greeting_path:
            candidate = os.path.expanduser(str(self.greeting_path))
            if os.path.isfile(candidate):
                source = candidate
            else:
                logger.warning("Reticulum: call greeting %s not found", candidate)
        cached = os.path.join(self._calls_dir(), f"greeting-{frame_ms}ms.ogg")
        if os.path.isfile(cached):
            return cached
        try:
            os.makedirs(self._calls_dir(), exist_ok=True)
            command = [ffmpeg, "-v", "error", "-y"]
            if source:
                command += ["-i", source]
            else:
                # Duration is a multiple of every Opus frame size in use.
                command += ["-f", "lavfi", "-i",
                            f"sine=frequency={BEEP_FREQUENCY}:duration=0.36:sample_rate=48000"]
            command += ["-ac", "1", "-c:a", "libopus", "-b:a", "24k",
                        "-application", "voip", "-frame_duration", str(frame_ms),
                        "-f", "ogg", cached]
            result = subprocess.run(command, capture_output=True, timeout=30,
                                    stdin=subprocess.DEVNULL, check=False)
            if result.returncode == 0 and os.path.getsize(cached) > 0:
                return cached
            logger.warning("Reticulum: could not write the call tone: %s",
                           (result.stderr or b"").decode("utf-8", "replace")[:200])
        except Exception as exc:
            logger.warning("Reticulum: could not generate the call tone: %s", exc)
        return None

    # -- call handling -------------------------------------------------------

    def _caller_allowed(self, identity_hash) -> bool:
        try:
            allowed = bool(self.allowed(bytes(identity_hash).hex()))
        except Exception:
            logger.exception("Reticulum: caller allow-list check failed")
            allowed = False
        logger.info("Reticulum: call from %s %s", bytes(identity_hash).hex()[:12],
                    "allowed" if allowed else "not on the allow-list")
        return allowed

    def _on_ringing(self, identity) -> None:
        caller = bytes(identity.hash).hex()
        path = os.path.join(
            self._calls_dir(),
            f"call_{caller[:12]}_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.ogg")
        os.makedirs(self._calls_dir(), exist_ok=True)
        with self._lock:
            self._call = {"caller": caller, "path": path, "started_at": time.time(),
                          "answered_at": None}
            sink = self._sink
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
        logger.info("Reticulum: call with %s established; recording to %s",
                    call["caller"], call["path"])
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
                if not self._call:
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
        with self._lock:
            call = self._call
            self._call = None
            sink = self._sink
            self._sink = None
        frames = int(getattr(sink, "frame_count", 0) or 0) if sink is not None else 0
        if sink is not None:
            try:
                sink.stop()  # drains and finalizes the Ogg/Opus writer
            except Exception:
                logger.exception("Reticulum: could not finalize the call recording")
        if not call:
            return
        if caller_override:
            call["caller"] = caller_override
        call["ended_at"] = time.time()
        call["duration"] = max(0.0, call["ended_at"] - (call.get("answered_at") or call["started_at"]))
        call["reason"] = reason
        path = call.get("path")
        if not path or not os.path.isfile(path) or os.path.getsize(path) <= 0:
            logger.info("Reticulum: call from %s produced no audio (%d frames captured)",
                        call["caller"], frames)
            return
        logger.info("Reticulum: call from %s recorded %.1fs (%d bytes); transcribing",
                    call["caller"], call["duration"], os.path.getsize(path))
        threading.Thread(target=self._transcribe_and_deliver, args=(call,),
                         name="lxst-call-transcribe", daemon=True).start()

    def _transcribe_and_deliver(self, call: dict) -> None:
        transcript = ""
        error = ""
        try:
            from tools.transcription_tools import transcribe_audio
            result = transcribe_audio(call["path"], source="gateway")
            if result.get("success"):
                transcript = str(result.get("transcript") or "").strip()
            else:
                error = str(result.get("error") or "transcription failed")
        except Exception as exc:
            error = f"transcription failed: {exc}"
            logger.exception("Reticulum: call transcription raised")
        call["transcript"] = transcript
        call["error"] = error
        try:
            self.on_transcript(call)
        except Exception:
            logger.exception("Reticulum: could not hand the call transcript to the adapter")

    # -- helpers -------------------------------------------------------------

    def _calls_dir(self) -> str:
        return os.path.join(os.path.expanduser(self.storage_path), "calls")

    def _scratch_path(self) -> str:
        # A sink may be created at ringing, before the caller is known. Point it
        # at a throwaway name; _on_ringing retargets it at the real call file.
        os.makedirs(self._calls_dir(), exist_ok=True)
        return os.path.join(self._calls_dir(),
                            f"unused_{tempfile._get_candidate_names().__next__()}.ogg")
