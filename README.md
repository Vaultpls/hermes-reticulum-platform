# hermes-reticulum-platform

Reticulum / LXMF platform adapter for [Hermes Agent](https://hermes-agent.nousresearch.com/docs). It turns a Hermes gateway into an LXMF delivery endpoint on a Reticulum mesh, so Sideband, Nomad Network and other LXMF clients can message the agent over LoRa, TCP, I2P or any Reticulum transport. No central messaging server is involved.

Version 2.0.0. Text only. Allow-listed by LXMF destination hash. Optional store-and-forward through an LXMF propagation node.

## How LXMF works

Reticulum is a cryptography-based mesh networking stack. Every participant has an identity, which is an Ed25519 keypair, and a destination hash derived from it. LXMF is the messaging layer on top: a sender addresses a 32 character hex destination hash, encrypts the payload to that identity's public key, signs it with its own key, and hands it to the local RNS instance. RNS finds a path through whatever interfaces are configured, which may be a LoRa radio, a TCP link, an I2P tunnel or a chain of transport nodes. When a path exists, the message travels DIRECT and the receiver returns a delivery proof. When the peer is offline, the message can be held by a propagation node and picked up later, which is the store-and-forward mode. Clients announce themselves periodically; an announce carries the display name and lets other nodes learn the path and public key.

## Requirements

- A working Reticulum config at `~/.reticulum` (or set `RETICULUM_CONFIG`).
- The Python packages `rns` and `lxmf` installed into the same Python that runs the Hermes gateway.
- At least one peer that allow-lists this endpoint's LXMF address.

## Install

1. Install the Reticulum packages into the Hermes runtime Python:

   ```
   pip install rns lxmf
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
| `RETICULUM_ANNOUNCE_INTERVAL` | `900` | Seconds between delivery announces. Values below 300 are raised to 300 |
| `RETICULUM_PROPAGATION_NODE` | empty | Propagation node hash for store-and-forward. Auto-selected from announces when blank |
| `RETICULUM_AUTO_PROPAGATION_NODE` | `true` | Learn a propagation node from live announces when none is pinned |
| `RETICULUM_PROPAGATION_SYNC_INTERVAL` | `60` | Seconds between LXMF propagation-node sync pulls (requesting mail queued for this destination). `0` disables |
| `RETICULUM_PROPAGATED_FALLBACK` | `true` | Retry a failed DIRECT send through a propagation node |
| `RETICULUM_DELIVERY_TIMEOUT` | `150` | Seconds to wait for a DIRECT delivery proof |
| `RETICULUM_PROPAGATED_TIMEOUT` | `180` | Seconds to wait for propagation node acceptance |
| `RETICULUM_DENY_REPLY` | `false` | Reply "Access not authorized." to non allow-listed senders |
| `RETICULUM_HOME_CHANNEL` | empty | LXMF hash used for cron and notification delivery |
| `RETICULUM_HOME_CHANNEL_NAME` | the hash | Human label for the home channel |
| `HERMES_RETICULUM_TEXT_BATCH_DELAY` | `1.5` | Quiet period used to merge rapid inbound messages into one turn |

With no allowlist and no `RETICULUM_ALLOW_ALL_USERS`, the DM policy is `disabled` and every inbound message is ignored. Group policy is always `disabled`; LXMF has no group concept.

## Security model

Three checks stand between the mesh and the agent.

- **Signature validation.** Only messages with `signature_validated` true are accepted. Anything else is dropped with a warning. This matters because LXMF lets a sender put an arbitrary `source_hash` in the envelope. If an unknown source identity could not be verified, anyone could claim to be an allow-listed peer. An unverifiable message is never attributed and never reaches the agent.
- **Allowlist.** After validation the source hash is normalized to lowercase hex and checked against `RETICULUM_ALLOWED_USERS`, or accepted wholesale when `RETICULUM_ALLOW_ALL_USERS` is set. The allowlist is read live on every message, so revoking an entry in config.yaml takes effect without a restart. Denied senders are ignored, and with `RETICULUM_DENY_REPLY` enabled they get one short "Access not authorized." reply.
- **Text sanitization.** Inbound bodies are stringified, title prefixed if present, and empty messages dropped. Outbound bodies have `MEDIA:` directives stripped, Markdown reduced to plain text (code fences, emphasis, inline code, images, links, headings), and long replies split at word boundaries with `[i/n]` tags. Attachments are not delivered; a content-only attachment produces a single notice that the bridge is text only.

Keys live on disk. The identity is written to `<storage>/hermes_identity` with mode `0600` on creation. An unreadable identity is moved aside to `hermes_identity.corrupt-<timestamp>` and a new one is generated, which changes the address; peers have to allow-list the new hash.

Stamp cost (default 8) adds a proof-of-work requirement to inbound messages and raises the cost of spamming a public endpoint. `0` turns it off.

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

Outbound replies are split at 1000 characters by default. `max_message_length` in `platforms.reticulum.extra` can raise this to 8000. Inbound messages are not length-limited by the adapter. There is no typing indicator, no read receipt, and no attachment support; the adapter implements `send_typing()` as a no-op because LXMF has no such concept.

Delivery is DIRECT first. If no proof arrives within `RETICULUM_DELIVERY_TIMEOUT`, and `RETICULUM_PROPAGATED_FALLBACK` is on, the adapter retries through a propagation node. A send fails cleanly with a reason string when the peer has not announced, no path is found, or the recalled identity does not match the destination hash.

## Self-test

`scripts/selftest.py` runs a full round trip over loopback TCP with no public transport nodes and no radio hardware. It boots the real adapter in one process, has a bare LXMF peer send `ping` from another, and checks that the echo comes back.

```
python3 scripts/selftest.py
```

It needs `rns` and `lxmf` in the current Python and the Hermes source tree importable. If the gateway source is not at `~/.hermes/hermes-agent`, set `HERMES_SRC` to its location. Exit code 0 means the round trip worked. Set `KEEP_SELFTEST=1` to keep the temporary workdir with both logs.

## Credits

The LXMF delivery endpoint pattern and much of the operational knowledge here come from `rns_hermes_endpoint` by apolosan, which is MIT licensed. This plugin is a rewrite against the Hermes v2 platform plugin API, but the approach to identity persistence, announce handling and propagation fallback follows that work.

Built on [Reticulum](https://reticulum.network) and [LXMF](https://github.com/markqvist/LXMF) by Mark Qvist.

## License

MIT. See [LICENSE](LICENSE).
