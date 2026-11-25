"""In-call audio primitives for the LXST telephone bridge.

The bridge used to record a call as one Ogg file and transcribe it only after
the caller hung up.  This module holds the pieces needed to make a call
conversational: a receive-side recorder that also segments speech, a
queue-backed transmit source that can play TTS while the call is still live,
and small ffmpeg helpers shared by both directions.

Nothing here imports LXST at module import time.  ``lxst`` is optional for the
plugin, and the unit-level helpers (segmentation, PCM conversion) must remain
importable on a text-only host.
"""

from __future__ import annotations

import collections
import contextlib
import logging
import math
import os
import shutil
import subprocess
import tempfile
import threading
import time
import wave
from typing import Callable, Optional

import numpy as np

logger = logging.getLogger(__name__)

DEFAULT_SAMPLE_RATE = 48_000
DEFAULT_FRAME_MS = 60
DEFAULT_ENERGY_THRESHOLD = 0.010
DEFAULT_SILENCE_MS = 650
DEFAULT_MIN_SPEECH_MS = 350
DEFAULT_MAX_UTTERANCE_MS = 20_000
DEFAULT_PRE_ROLL_MS = 300
MAX_OUTBOUND_SECONDS = 90.0


def _as_mono(samples) -> np.ndarray:
    """Return float32 mono samples shaped as one column."""
    frame = np.asarray(samples, dtype=np.float32)
    if frame.ndim == 1:
        frame = frame.reshape(-1, 1)
    elif frame.ndim > 2:
        frame = frame.reshape(frame.shape[0], -1)
    if frame.shape[1] > 1:
        frame = frame.mean(axis=1, keepdims=True)
    return np.clip(frame, -1.0, 1.0)


def _frame_rms(frame: np.ndarray) -> float:
    if frame.size == 0:
        return 0.0
    return float(np.sqrt(np.mean(np.square(frame, dtype=np.float64))))


class UtteranceSegmenter:
    """Split a continuous call into speech utterances using energy VAD.

    Telephony codecs already discard most background noise, so an adaptive
    RMS gate is sufficient and far cheaper than a neural VAD.  The gate follows
    the quiet floor, requires a short attack, keeps a pre-roll so word onsets
    are not clipped, and closes an utterance after a configurable pause.
    """

    def __init__(
        self,
        on_segment: Callable[[np.ndarray, int], None],
        *,
        sample_rate: Optional[int] = None,
        energy_threshold: float = DEFAULT_ENERGY_THRESHOLD,
        silence_ms: float = DEFAULT_SILENCE_MS,
        min_speech_ms: float = DEFAULT_MIN_SPEECH_MS,
        max_utterance_ms: float = DEFAULT_MAX_UTTERANCE_MS,
        pre_roll_ms: float = DEFAULT_PRE_ROLL_MS,
        on_speech_start: Optional[Callable[[], None]] = None,
    ):
        self.on_segment = on_segment
        self.on_speech_start = on_speech_start
        self.sample_rate = int(sample_rate or 0) or None
        self.energy_threshold = max(1e-5, float(energy_threshold))
        self.silence_ms = max(100.0, float(silence_ms))
        self.min_speech_ms = max(50.0, float(min_speech_ms))
        self.max_utterance_ms = max(500.0, float(max_utterance_ms))
        self.pre_roll_ms = max(0.0, float(pre_roll_ms))

        self._lock = threading.Lock()
        self._pre_roll: collections.deque[np.ndarray] = collections.deque()
        self._pre_roll_samples = 0
        self._buffer: list[np.ndarray] = []
        self._buffer_samples = 0
        self._speech_samples = 0
        self._silence_ms = 0.0
        self._noise_floor = self.energy_threshold * 0.5
        self._active = False
        self._speech_run_ms = 0.0

    @property
    def active(self) -> bool:
        with self._lock:
            return self._active

    def feed(self, samples, sample_rate: Optional[int] = None) -> None:
        frame = _as_mono(samples)
        if frame.size == 0:
            return
        rate = int(sample_rate or self.sample_rate or DEFAULT_SAMPLE_RATE)
        with self._lock:
            if self.sample_rate is None:
                self.sample_rate = rate
            duration_ms = 1000.0 * frame.shape[0] / max(1, rate)
            rms = _frame_rms(frame)

            # Follow the quiet floor only while no utterance is open; otherwise
            # a long, loud sentence would raise its own detection threshold.
            if not self._active:
                self._noise_floor = (0.92 * self._noise_floor
                                     + 0.08 * min(rms, self.energy_threshold))
            threshold = max(self.energy_threshold, self._noise_floor * 2.5)
            is_speech = rms >= threshold

            if not self._active:
                self._pre_roll.append(frame)
                self._pre_roll_samples += frame.shape[0]
                limit = int(rate * self.pre_roll_ms / 1000.0)
                while self._pre_roll and self._pre_roll_samples > limit:
                    dropped = self._pre_roll.popleft()
                    self._pre_roll_samples -= dropped.shape[0]
                if is_speech:
                    self._speech_run_ms += duration_ms
                    if self._speech_run_ms >= min(100.0, self.silence_ms * 0.3):
                        self._start_locked(rate)
                else:
                    self._speech_run_ms = 0.0
                return

            self._buffer.append(frame)
            self._buffer_samples += frame.shape[0]
            if is_speech:
                self._speech_samples += frame.shape[0]
                self._silence_ms = 0.0
            else:
                self._silence_ms += duration_ms

            should_emit = (
                self._silence_ms >= self.silence_ms
                or (1000.0 * self._buffer_samples / max(1, rate)) >= self.max_utterance_ms
            )
            if should_emit:
                self._emit_locked(rate)

    def _start_locked(self, rate: int) -> None:
        self._active = True
        self._buffer = list(self._pre_roll)
        self._buffer_samples = self._pre_roll_samples
        self._pre_roll.clear()
        self._pre_roll_samples = 0
        self._speech_samples = self._buffer_samples
        self._silence_ms = 0.0
        self._speech_run_ms = 0.0
        logger.debug("Reticulum: utterance started (rms floor %.4f)",
                     self._noise_floor)
        callback = self.on_speech_start
        if callback is not None:
            try:
                callback()
            except Exception:
                logger.debug("Reticulum: speech-start callback failed", exc_info=True)

    def _emit_locked(self, rate: int) -> None:
        chunks = self._buffer
        self._buffer = []
        self._buffer_samples = 0
        self._active = False
        speech_ms = 1000.0 * self._speech_samples / max(1, rate)
        self._speech_samples = 0
        closing_silence_ms = self._silence_ms
        self._silence_ms = 0.0
        if not chunks:
            return
        # Keep only a short tail of the closing pause for STT; a long pause
        # must not become part of the next model input.
        trailing_ms = max(0.0, closing_silence_ms - min(160.0, closing_silence_ms * 0.25))
        trim_samples = int(rate * trailing_ms / 1000.0)
        samples = np.concatenate(chunks, axis=0)
        if trim_samples and samples.shape[0] > trim_samples:
            samples = samples[: samples.shape[0] - trim_samples]
        if speech_ms < self.min_speech_ms:
            logger.debug("Reticulum: dropping %.0fms utterance below %.0fms minimum",
                         speech_ms, self.min_speech_ms)
            return
        try:
            self.on_segment(samples, rate)
        except Exception:
            logger.exception("Reticulum: utterance callback failed")

    def flush(self) -> None:
        """Emit an open utterance, used when the call ends mid-sentence."""
        with self._lock:
            if not self._active:
                return
            rate = self.sample_rate or DEFAULT_SAMPLE_RATE
            self._emit_locked(rate)


def encode_pcm_to_ogg(path: str, samples, sample_rate: int,
                      bitrate: str = "24k", timeout: int = 90) -> bool:
    """Write float PCM as mono Ogg/Opus. Returns False when ffmpeg is absent."""
    frame = _as_mono(samples)
    if frame.size == 0:
        return False
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        logger.warning("Reticulum: ffmpeg is required to encode call audio")
        return False
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    wav_path = f"{path}.wav"
    pcm = (frame * 32767.0).astype("<i2")
    try:
        with wave.open(wav_path, "wb") as handle:
            handle.setnchannels(1)
            handle.setsampwidth(2)
            handle.setframerate(int(sample_rate or DEFAULT_SAMPLE_RATE))
            handle.writeframes(pcm.tobytes())
        result = subprocess.run(
            [ffmpeg, "-v", "error", "-y", "-i", wav_path, "-ac", "1",
             "-c:a", "libopus", "-b:a", bitrate, "-application", "voip",
             "-f", "ogg", path],
            capture_output=True, timeout=timeout, stdin=subprocess.DEVNULL, check=False)
        if result.returncode == 0 and os.path.getsize(path) > 0:
            return True
        logger.warning("Reticulum: could not encode call audio: %s",
                       (result.stderr or b"").decode("utf-8", "replace")[:200])
        return False
    except Exception as exc:
        logger.warning("Reticulum: could not encode call audio: %s", exc)
        return False
    finally:
        with contextlib.suppress(OSError):
            os.unlink(wav_path)


def decode_audio_to_pcm(path: str, sample_rate: int = DEFAULT_SAMPLE_RATE,
                        timeout: int = 60) -> Optional[np.ndarray]:
    """Decode any ffmpeg-readable file to mono float32 at ``sample_rate``."""
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg or not path or not os.path.isfile(path):
        return None
    try:
        result = subprocess.run(
            [ffmpeg, "-v", "error", "-i", os.path.abspath(path), "-vn",
             "-ac", "1", "-ar", str(int(sample_rate)), "-f", "f32le",
             "-acodec", "pcm_f32le", "pipe:1"],
            capture_output=True, timeout=timeout, stdin=subprocess.DEVNULL, check=False)
        if result.returncode != 0 or not result.stdout:
            logger.warning("Reticulum: could not decode call audio %s: %s",
                           path, (result.stderr or b"").decode("utf-8", "replace")[:200])
            return None
        return np.frombuffer(result.stdout, dtype="<f4").reshape(-1, 1).copy()
    except Exception as exc:
        logger.warning("Reticulum: could not decode call audio %s: %s", path, exc)
        return None


def make_call_recorder(path: str, segmenter: Optional[UtteranceSegmenter] = None):
    """A receive sink that archives a call and optionally streams utterances."""
    return _recorder_class()(path, segmenter)


def _recorder_class():
    global _RECORDER_CLASS
    if _RECORDER_CLASS is not None:
        return _RECORDER_CLASS
    from LXST.Sinks import Sink

    class _CallRecorder(Sink):
        """Collect decoded call frames, record locally, feed the VAD."""

        def __init__(self, output_path, utterance_segmenter=None):
            self.path = output_path
            self.segmenter = utterance_segmenter
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
            self._next_frame_at = 0.0

        def can_receive(self, from_source=None) -> bool:  # type: ignore[override]
            if not self.should_run:
                return False
            # LXST's Mixer has no clock of its own; it pumps frames as fast as
            # its sink accepts them. A recorder that always says yes inflates a
            # four-second call into twenty-five seconds of audio and destroys
            # the VAD's timing. Pace acceptance by the actual frame duration.
            with self._lock:
                return time.monotonic() >= self._next_frame_at

        def handle_frame(self, frame, source=None):
            if isinstance(frame, (bytes, bytearray, memoryview)):
                frame = np.frombuffer(bytes(frame), dtype="<f4")
            samples = _as_mono(frame)
            if samples.size == 0:
                return
            rate = int(getattr(source, "samplerate", None) or DEFAULT_SAMPLE_RATE)
            with self._lock:
                if self.frame_count == 0:
                    logger.info("Reticulum: call audio frame %s from %s at %s Hz",
                                samples.shape, type(source).__name__,
                                getattr(source, "samplerate", None))
                self._chunks.append(samples)
                self.frame_count += 1
                if self.samplerate is None:
                    self.samplerate = rate
                duration = samples.shape[0] / max(1, rate)
                now = time.monotonic()
                self._next_frame_at = max(now, self._next_frame_at) + duration
            if self.segmenter is not None:
                try:
                    self.segmenter.feed(samples, rate)
                except Exception:
                    logger.exception("Reticulum: call VAD failed")

        def wait_for_frames(self):
            return None

        def enable_low_latency(self):
            return None

        def start(self):
            return None

        def stop(self):
            self.should_run = False
            if self.segmenter is not None:
                self.segmenter.flush()
            with self._lock:
                chunks = self._chunks
                self._chunks = []
            logger.info("Reticulum: call recorder stopping %s with %d frames",
                        self.path, self.frame_count)
            if not chunks:
                return
            samples = np.concatenate(chunks, axis=0)
            rate = int(self.samplerate or DEFAULT_SAMPLE_RATE)
            encode_pcm_to_ogg(self.path, samples, rate)

    _RECORDER_CLASS = _CallRecorder
    return _RECORDER_CLASS


_RECORDER_CLASS = None
_SOURCE_CLASS = None


class CallAudioBuffer:
    """Thread-safe PCM queue shared by every transmit source in one call."""

    def __init__(self):
        self._lock = threading.Lock()
        self._chunks: collections.deque[np.ndarray] = collections.deque()
        self._samples = 0
        self._greeting: list[np.ndarray] = []

    def set_greeting(self, samples) -> None:
        chunk = _as_mono(samples)
        with self._lock:
            self._greeting = [chunk] if chunk.size else []
            self._samples = sum(item.shape[0] for item in self._chunks)

    def enqueue(self, samples) -> int:
        chunk = _as_mono(samples)
        if chunk.size == 0:
            return 0
        limit = int(DEFAULT_SAMPLE_RATE * MAX_OUTBOUND_SECONDS)
        with self._lock:
            self._chunks.append(chunk)
            self._samples += chunk.shape[0]
            while self._chunks and self._samples > limit:
                dropped = self._chunks.popleft()
                self._samples -= dropped.shape[0]
        return chunk.shape[0]

    def clear(self) -> None:
        with self._lock:
            self._chunks.clear()
            self._samples = 0
            self._greeting = []

    @property
    def pending_samples(self) -> int:
        with self._lock:
            return self._samples

    def take(self, count: int) -> np.ndarray:
        """Pop exactly ``count`` samples, zero-padding when the queue is empty."""
        count = max(0, int(count))
        out = np.zeros((count, 1), dtype=np.float32)
        with self._lock:
            if self._greeting:
                greeting = self._greeting[0]
                take = min(count, greeting.shape[0])
                out[:take] = greeting[:take]
                if take == greeting.shape[0]:
                    self._greeting = []
                else:
                    self._greeting[0] = greeting[take:]
                return out
            filled = 0
            while filled < count and self._chunks:
                chunk = self._chunks[0]
                take = min(count - filled, chunk.shape[0])
                out[filled:filled + take] = chunk[:take]
                filled += take
                if take == chunk.shape[0]:
                    self._chunks.popleft()
                    self._samples -= chunk.shape[0]
                else:
                    self._chunks[0] = chunk[take:]
                    self._samples -= take
            return out


def make_call_audio_source(*, target_frame_ms: int, codec, sink, buffer: CallAudioBuffer):
    """Create a paced LXST source that plays queued TTS and silence otherwise."""
    return _source_class()(target_frame_ms=target_frame_ms, codec=codec,
                           sink=sink, buffer=buffer)


def _source_class():
    global _SOURCE_CLASS
    if _SOURCE_CLASS is not None:
        return _SOURCE_CLASS
    from LXST.Sources import LocalSource

    class _CallAudioSource(LocalSource):
        """A microphone replacement with a live, refillable playback queue.

        LXST expects a Source whose thread pushes frames into the transmit
        mixer.  Emitting silence when the queue is empty keeps that pipeline
        alive; TTS frames are emitted at the negotiated cadence as they arrive.
        """

        def __init__(self, target_frame_ms=DEFAULT_FRAME_MS, codec=None,
                     sink=None, buffer=None):
            self.target_frame_ms = max(10, int(target_frame_ms or DEFAULT_FRAME_MS))
            self.samplerate = DEFAULT_SAMPLE_RATE
            self.channels = 1
            self.bitdepth = 32
            self.should_run = False
            self.ingest_thread = None
            self._codec = codec
            self._sink = sink
            self._buffer = buffer or CallAudioBuffer()
            self.samples_per_frame = math.ceil(
                self.target_frame_ms / 1000.0 * DEFAULT_SAMPLE_RATE)
            self.frame_time = self.samples_per_frame / DEFAULT_SAMPLE_RATE
            self.frames_sent = 0
            self._stop_event = threading.Event()

        @property
        def codec(self): return self._codec

        @codec.setter
        def codec(self, value): self._codec = value

        @property
        def sink(self): return self._sink

        @sink.setter
        def sink(self, value): self._sink = value

        @property
        def running(self): return self.should_run

        def start(self):
            if not self.should_run:
                self.should_run = True
                self._stop_event.clear()
                self.ingest_thread = threading.Thread(
                    target=self._ingest_job, name="lxst-call-audio", daemon=True)
                self.ingest_thread.start()

        def stop(self):
            self.should_run = False
            self._stop_event.set()

        def _ingest_job(self):
            next_frame = time.monotonic()
            try:
                while self.should_run:
                    if self._sink is None or self._sink.can_receive(from_source=self):
                        frame = self._buffer.take(self.samples_per_frame)
                        if self._codec:
                            frame = self._codec.encode(frame)
                        if self._sink is not None:
                            self._sink.handle_frame(frame, self)
                            self.frames_sent += 1
                            if self.frames_sent % 20 == 0:
                                logger.debug("Reticulum: in-call source sent %d frames",
                                             self.frames_sent)
                    next_frame += self.frame_time
                    delay = next_frame - time.monotonic()
                    if delay > 0:
                        self._stop_event.wait(delay)
                    else:
                        next_frame = time.monotonic()
            except Exception:
                logger.exception("Reticulum: in-call audio source stopped after %d frames",
                                 self.frames_sent)
                self.should_run = False

    _SOURCE_CLASS = _CallAudioSource
    return _SOURCE_CLASS
