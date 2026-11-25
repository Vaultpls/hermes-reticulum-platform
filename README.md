# hermes-reticulum-platform

Reticulum / LXMF platform adapter for [Hermes Agent](https://hermes-agent.nousresearch.com/docs). It turns a Hermes gateway into an LXMF delivery endpoint on a Reticulum mesh, so Sideband, Nomad Network and other LXMF clients can message the agent over LoRa, TCP, I2P or any Reticulum transport. No central messaging server is involved.

Version 2.2.0. Text, voice notes and LXST telephone calls. Allow-listed by LXMF destination hash. Optional store-and-forward through an LXMF propagation node. Every message carries a UTC timestamp, and out-of-order arrivals are detected and ordered by the sender's own LXMF timestamp.

## How LXMF works

Reticulum is a cryptography-based mesh networking stack. Every participant has an identity, which is an Ed25519 keypair, and a destination hash derived from it. LXMF is the messaging layer on top: a sender addresses a 32 character hex destination hash, encrypts the payload to that identity's public key, signs it with its own key, and hands it to the local RNS instance. RNS finds a path through whatever interfaces are configured, which may be a LoRa radio, a TCP link, an I2P tunnel or a chain of transport nodes. When a path exists, the message travels DIRECT and the receiver returns a delivery proof. When the peer is offline, the message can be held by a propagation node and picked up later, which is the store-and-forward mode. Clients announce themselves periodically; an announce carries the display name and lets other nodes learn the path and public key.

## Requirements

- A working Reticulum config at `~/.reticulum` (or set `RETICULUM_CONFIG`).
- The Python packages `rns` and `lxmf` installed into the same Python that runs the Hermes gateway.
- At least one peer that allow-lists this endpoint's LXMF address.
- For telephone calls: the `lxst` package (`pip install lxst`, pulls `pycodec2`) and the system Opus libraries (`libopus0`, `libopusfile0`) on the gateway host. Without them the adapter logs one line and keeps running text-only.
- For voice: `ffmpeg` on the gateway host (Codec2 ↔ Opus; most distro builds include libcodec2 and libopus) and a speech-to-text backend in Hermes. `stt.enabled: true` with `stt.provider: local` uses faster-whisper, which Hermes installs on first use; text-only operation needs neither.

## Install

1. Install the Reticulum packages into the Hermes runtime Python:

   ```
   pip install rns lxmf lxst
   ```

   Use the interpreter that runs the gateway (the venv or system Python behind `hermes`), not a separate environment.

2. Put the plugin directory where Hermes looks for platform plugins:

   ```
   cp -r hermes-reticulum-platform ~/.hermes/plugins/reticulum-platform
   ```

   A symlink works too. The directory name should match the plugin name in `plugin.yaml`.

3. Enable the plugin. Plugins are disabled by default, so it has to be turned on in config.yaml. The CLI does the edit for you:

   ```
   hermes plugins enable reticulum-platform
   ```

   You can also open `hermes config` and enable it there. The same screen prompts for the `RETICULUM_*` variables listed below.

4. Restart the gateway:

   ```
   hermes gateway restart
   ```

   On startup the adapter creates its identity if one does not exist, announces the delivery destination, and logs the LXMF address. Give that address to your peers so they can allow-list it.

`pip install` must happen before the plugin can connect. When `RNS` or `LXMF` is missing, `connect()` fails fast with `deps_missing` and the fix hint `pip install rns lxmf`; it does not retry.

## Configuration

Environment variables take precedence over `platforms.reticulum.extra` in config.yaml. All of them are optional.

| Variable | Default | Meaning |
| --- | --- | --- |
| `RETICULUM_DISPLAY_NAME` | `Hermes` | Name announced on the mesh |
| `RETICULUM_STORAGE` | `~/.lxmf/storage` | Directory for the LXMF identity and router state |
| `RETICULUM_CONFIG` | `~/.reticulum` or `/etc/reticulum` | Reticulum config directory |
| `RETICULUM_ALLOWED_USERS` | empty (deny all) | Comma-separated LXMF destination hashes allowed to talk to the bot |
| `RETICULUM_ALLOW_ALL_USERS` | `false` | Allow any destination hash. Dev only, disables the allowlist |
| `RETICULUM_STAMP_COST` | `8` | Proof-of-work stamp cost required from senders. `0` disables, clamped to 64 |
| `RETICULUM_ANNOUNCE_INTERVAL` | `3600` | Seconds between delivery announces. Values below 3600 are raised to 3600; RNS rate-limits announces per destination (`AR_TARGET`), so faster announcing gets the destination blocked on the mesh |
| `RETICULUM_PROPAGATION_NODE` | empty | Propagation node hash for store-and-forward. Auto-selected from announces when blank |
| `RETICULUM_AUTO_PROPAGATION_NODE` | `true` | Learn a propagation node from live announces when none is pinned |
| `RETICULUM_PROPAGATION_SYNC_INTERVAL` | `60` | Seconds between LXMF propagation-node sync pulls (requesting mail queued for this destination). `0` disables |
| `RETICULUM_PROPAGATED_FALLBACK` | `true` | Retry a failed DIRECT send through a propagation node |
| `RETICULUM_DELIVERY_TIMEOUT` | `150` | Seconds to wait for a DIRECT delivery proof |
| `RETICULUM_PROPAGATED_TIMEOUT` | `180` | Seconds to wait for propagation node acceptance |
| `RETICULUM_DENY_REPLY` | `false` | Reply "Access not authorized." to non allow-listed senders |
| `RETICULUM_HOME_CHANNEL` | empty | LXMF hash used for cron and notification delivery |
| `RETICULUM_HOME_CHANNEL_NAME` | the hash | Human label for the home channel |
| `RETICULUM_TIMESTAMPS` | `true` | Prefix every outbound body with a UTC send timestamp and stamp inbound bodies with the sender's timestamp |
| `RETICULUM_REORDER_WINDOW` | `1.0` | Seconds to hold inbound messages so late arrivals can be sorted by timestamp before dispatch. `0` disables reordering (late arrivals are still flagged) |
| `RETICULUM_AUDIO_BITRATE` | `24k` | Opus bitrate for outbound voice notes |
| `RETICULUM_TTS_MAX_BYTES` | `98304` | Largest outbound voice note in bytes (mesh budget). Over-budget clips are retried at 12k, then reported |
| `RETICULUM_MAX_AUDIO_BYTES` | `4194304` | Largest inbound voice clip accepted, before Codec2 transcoding |
| `RETICULUM_TELEPHONE` | `true` | Announce and answer an LXST telephone destination (needs `lxst`) |
| `RETICULUM_CALL_MAX_SECONDS` | `300` | Longest recorded call before the adapter hangs up |
| `RETICULUM_CALL_PROFILE` | `auto` | Cap inbound call bandwidth (`ultra_low`, `very_low`, `low`, `medium`, `high`, `max`); `auto` honours the caller |
| `RETICULUM_CALL_GREETING_TEXT` | `Hermes here. Go ahead.` | Words spoken on answer (rendered with edge-tts, cached) |
| `RETICULUM_CALL_GREETING` | unset | Optional Ogg/Opus file played when a call is answered (overrides the spoken greeting) |
| `RETICULUM_CALL_GREETING_VOICE` | `en-US-AriaNeural` | edge-tts voice used for the spoken greeting |
| `RETICULUM_CALL_ALLOW_PREFIXES` | unset | Comma-separated caller-hash prefixes allowed despite not being on the allow-list (dev bridge; exact hashes preferred) |
| `HERMES_RETICULUM_TEXT_BATCH_DELAY` | `1.5` | Quiet period used to merge rapid inbound messages into one turn |

With no allowlist and no `RETICULUM_ALLOW_ALL_USERS`, the DM policy is `disabled` and every inbound message is ignored. Group policy is always `disabled`; LXMF has no group concept.

## Security model

Three checks stand between the mesh and the agent.

- **Signature validation.** Only messages with `signature_validated` true are accepted. Anything else is dropped with a warning. This matters because LXMF lets a sender put an arbitrary `source_hash` in the envelope. If an unknown source identity could not be verified, anyone could claim to be an allow-listed peer. An unverifiable message is never attributed and never reaches the agent.
- **Allowlist.** After validation the source hash is normalized to lowercase hex and checked against `RETICULUM_ALLOWED_USERS`, or accepted wholesale when `RETICULUM_ALLOW_ALL_USERS` is set. The allowlist is read live on every message, so revoking an entry in config.yaml takes effect without a restart. Denied senders are ignored, and with `RETICULUM_DENY_REPLY` enabled they get one short "Access not authorized." reply.
- **Text sanitization.** Inbound bodies are stringified, title prefixed if present, and empty messages dropped. Audio attachments are size-capped and cached; everything else is ignored. Outbound bodies have `MEDIA:` directives stripped, Markdown reduced to plain text (code fences, emphasis, inline code, images, links, headings), stamped with a UTC send time, and long replies split at word boundaries with `[i/n]` tags.

Keys live on disk. The identity is written to `<storage>/hermes_identity` with mode `0600` on creation. An unreadable identity is moved aside to `hermes_identity.corrupt-<timestamp>` and a new one is generated, which changes the address; peers have to allow-list the new hash.

Stamp cost (default 8) adds a proof-of-work requirement to inbound messages and raises the cost of spamming a public endpoint. `0` turns it off.

## Voice notes: STT in, TTS out

The adapter speaks the LXMF voice format, so a phone running Sideband can be used like a telephone: hold to talk, read the transcript, and hear the answer.

Inbound:
- A PTT clip arrives in `FIELD_AUDIO` as `[mode, bytes]`. `AM_OPUS_OGG` clips are cached as-is. Codec2 clips (Sideband's default when HQ PTT is off, because it survives LoRa) are decoded with ffmpeg's `codec2raw` demuxer and re-encoded to Ogg/Opus. Modes 700C through 3200 are supported; 450/450PWB are reported back as unsupported instead of vanishing.
- Audio file attachments whose extension is a known audio type are cached the same way.
- The cached file is attached to the `MessageEvent` as `message_type=VOICE` with an `audio/*` MIME type. Hermes' own STT pipeline (`stt.provider` in config.yaml) then transcribes it exactly as it does Telegram or WhatsApp voice notes, before the agent runs. No STT is reimplemented here.
- A clip that cannot be decoded adds a short note to the turn, so neither side is left guessing why no transcript appeared.

Outbound:
- Every voice reply is Ogg/Opus (`AM_OPUS_OGG`), mono, 24 kbps voip profile, which Sideband plays natively.
- Enable voice replies per chat by sending `/voice on` (or set `voice.auto_tts: true` in config.yaml to make it the default). Voice input then gets a synthesized voice answer; the text answer is still sent, so the conversation stays readable on a small screen or a silent phone.
- `send_voice()` is a normal platform hook, so both auto-TTS and a `MEDIA:` audio file from a tool land here.
- Clips over `RETICULUM_TTS_MAX_BYTES` are re-encoded once at 12 kbps and, if still too large, are skipped with a logged reason. A long spoken answer must not turn into a multi-minute LoRa transfer; the text reply still goes out.

The voice note's body is a timestamped one-liner (`[2026-11-25 15:04:05Z] [voice message]`), and the reply anchor is carried in `FIELD_REPLY_TO` so the phone can thread it against the message it answers.

## Telephone calls (LXST)

Sideband can dial the agent. The adapter announces a second destination derived from the same identity, aspect `lxst.telephony`; for the current `~/.lxmf/storage` identity that address is printed in the gateway log as `Reticulum: telephone ready at <hash>`.

- Only allow-listed callers are answered (`RETICULUM_ALLOWED_USERS`, same list as text). Everyone else gets the normal LXST rejection.
- The call is answered after one ring, a spoken greeting plays, and the caller's audio is recorded to `<storage>/calls/call_<caller>_<utc>.ogg`.
- On hangup the recording is transcribed with the gateway's STT backend and handed to the agent as an ordinary inbound turn, prefixed `[voice call, Ns]`. The agent's reply goes back over LXMF as a normal (optionally spoken) message, so the answer lands in the caller's chat.
- Calls longer than `RETICULUM_CALL_MAX_SECONDS` are hung up; recordings stay on disk for auditing. The host needs no sound card: the adapter substitutes file-backed LXST sources and sinks for the microphone and speaker.
- `RETICULUM_CALL_PROFILE` caps the bandwidth a call may use. `low` is Codec2 at 3.2 kbit/s (about 200 ms frames), `very_low` is 1.6 kbit/s and `ultra_low` is 700 bit/s. The caller's preference is downgraded and signalled back, so both ends use the same codec.
- If `lxst` is missing at startup the adapter retries every few minutes; installing the package does not require a config change.

Test it without a phone: `python scripts/phone_selftest.py` starts two Reticulum instances on loopback, dials the bridge, sends a tone, and checks that both sides recorded audio and the transcript callback fired.

## Timestamps and out-of-order delivery

Mesh links are delay-tolerant, so messages can arrive out of order: a retry over a slower path can land after a newer message, and store-and-forward mail can be delivered in a burst.

- Every outbound body is prefixed with a UTC timestamp: `[2026-11-25 15:04:05Z] text`. A split reply stamps every part and adds its index: `[2026-11-25 15:04:05Z 2/5]`. A client that receives parts out of order can sort them, and a human can see when each part was sent. Disable with `RETICULUM_TIMESTAMPS=false`.
- Every inbound body is prefixed with the sender's own LXMF timestamp, read from `message.timestamp`, plus the receive time in the event metadata. The agent therefore sees when the message was written, not just when it arrived.
- Inbound messages are held for `RETICULUM_REORDER_WINDOW` seconds per sender, sorted by `(timestamp, arrival sequence)`, then dispatched in that order. The window is short enough not to be felt on a normal link and long enough to absorb a retry.
- A message that arrives after the window has closed, with a timestamp older than the last message already handed to the agent, is dispatched immediately and flagged: `[2026-11-25 14:03:12Z | out of order, arrived 67s late]`. A turn cannot be rewound, so the flag plus the timestamp is what lets the agent place it correctly instead of treating it as the newest message.
- Duplicate deliveries of the same envelope (DIRECT proof plus store-and-forward copy) are dropped by LXMF message hash for 15 minutes, so the same user turn never runs twice.
- A sender whose clock runs fast cannot poison the lateness check: the baseline is capped at the receive time plus two seconds.

## Runtime stamp and name control

The plugin registers two Hermes tools so the agent can reconfigure the endpoint itself, without editing env files or restarting:

`reticulum_stamp_cost`
- `{"action": "get"}` returns `stamp_cost`, `enforcing_stamps`, the live address and the LXMF cap (100, so 99 is the maximum settable cost).
- `{"action": "set", "cost": 0}` disables stamps; any value up to 99 requires that many leading zero bits.
- A set call updates the LXMF router's inbound cost and toggles `enforce_stamps` to match, then re-announces so peers see the change immediately.

`reticulum_display_name`
- `{"action": "get"}` returns the currently announced name.
- `{"action": "set", "name": "Joshua"}` changes the name in the destination's announce app-data (what Sideband shows), 1-64 UTF-8 bytes, and re-announces.

Both tools persist by default: the value is written to `RETICULUM_STAMP_COST` / `RETICULUM_DISPLAY_NAME` in the Hermes `.env`, so it survives gateway restarts. Pass `"persist": false` for a live-only change. A restart still re-reads the env value at adapter construction, which is why persist exists.

## One RNS instance per storage dir

RNS is a process-wide singleton. A second `RNS.Reticulum()` in the same process raises, and the LXMF router keeps a SQLite store per storage directory. The adapter therefore caches the Reticulum instance, identity, router and delivery destination in a module-level dict keyed by the resolved storage path, and reconnects reuse that stack instead of re-initializing it.

Practically:

- Run one gateway process per storage directory. Two processes pointed at the same `RETICULUM_STORAGE` will fight over the identity file and the router store.
- If RNS is already initialized in the process with a different config, `RETICULUM_CONFIG` is ignored with a warning. RNS cannot be re-pointed after startup.
- `disconnect()` stops the announce loop and detaches the message sink but keeps the stack alive, so a gateway reconnect is cheap.

## Message limits

Outbound replies are split at 1000 characters by default. `max_message_length` in `platforms.reticulum.extra` can raise this to 8000. Inbound messages are not length-limited by the adapter. There is no typing indicator and no read receipt; the adapter implements `send_typing()` as a no-op because LXMF has no such concept. Images and generic files are not bridged; audio is (see the voice section), and a content-only attachment produces a short notice.

Delivery is DIRECT first. If no proof arrives within `RETICULUM_DELIVERY_TIMEOUT`, and `RETICULUM_PROPAGATED_FALLBACK` is on, the adapter retries through a propagation node. A send fails cleanly with a reason string when the peer has not announced, no path is found, or the recalled identity does not match the destination hash.

## Self-test

`scripts/selftest.py` runs offline helper checks and a full round trip over loopback TCP with no public transport nodes and no radio hardware. The helper pass covers timestamp formatting, the reorder buffer and late-arrival flagging, Codec2 decoding and Opus payload preparation. The process pair boots the real adapter in one process, has a bare LXMF peer send `ping` and an Opus voice note from another, and checks that the text reply is timestamped, the voice note reached the adapter as a `VOICE` event with a cached audio file, and a spoken reply came back as an `AM_OPUS_OGG` voice note.

```
python3 scripts/selftest.py
```

It needs `rns` and `lxmf` in the current Python and the Hermes source tree importable. If the gateway source is not at `~/.hermes/hermes-agent`, set `HERMES_SRC` to its location. Exit code 0 means the round trip worked. Set `KEEP_SELFTEST=1` to keep the temporary workdir with both logs.

## Credits

The LXMF delivery endpoint pattern and much of the operational knowledge here come from `rns_hermes_endpoint` by apolosan, which is MIT licensed. This plugin is a rewrite against the Hermes v2 platform plugin API, but the approach to identity persistence, announce handling and propagation fallback follows that work.

Built on [Reticulum](https://reticulum.network) and [LXMF](https://github.com/markqvist/LXMF) by Mark Qvist.

## License

MIT. See [LICENSE](LICENSE).
