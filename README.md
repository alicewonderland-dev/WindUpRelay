# WindUpRelay

Private relay service for the Wind-Up Key Dalamud plugin.

## Expected checkout layout

This repository is designed to be cloned as `WindUpRelay` inside a checkout of
the public `Wind-Up Key Plugin` repository. Its project reference expects the
public protocol project at:

`..\WindUpKey.Protocol\WindUpKey.Protocol.csproj`

## Local configuration

`appsettings.Production.json` is intentionally ignored because it contains the
relay token. Create and maintain that file locally; never commit it.

Build explicitly with:

`dotnet build WindUpRelay.csproj -c Release`

## Linux

`WindUpRelay.Host` is Windows-only. Use the Linux host GUI from the plugin repo root:

```bash
./Start-WindUpHost.sh
```

That opens a Start/Stop window, starts the Production relay on `:29417`, and enables
Tailscale Funnel (same flow as the Windows tray host).

Terminal-only alternative:

```bash
./Start-WindUpRelay.sh           # Production relay on :29417
./Start-WindUpRelay.sh --funnel  # also enable Tailscale Funnel
```

Requires the .NET SDK, `Relay:Token` in `appsettings.Production.json`, and
(for Funnel) the Tailscale CLI with Funnel allowed.

## Relay identity (`Relay:AuthMode`)

Each character's pairing key is its public account ID. To prove it owns that key,
the plugin generates a per-character secret (32 random bytes, unpadded base64url)
and sends it in `register`. The relay stores only `SHA-256(secret bytes)` as hex in
the `accounts` table and compares in constant time. It never stores or logs the raw
secret, and never logs the hash.

`Relay:AuthMode` (env `Relay__AuthMode`) picks how strict the relay is:

| Mode | Behaviour |
|---|---|
| `Off` | Pre-auth behaviour. Secrets are ignored. Use only to roll back. |
| `Bind` (default) | A register with a secret claims an unclaimed key (trust on first use), or must match the stored hash (otherwise `auth_failed` and close). A register without a secret is allowed only while the key is unclaimed; for a claimed key the socket is closed with `PolicyViolation` / `auth_required` and no error message, so old clients don't print chat errors. |
| `Enforce` | Like `Bind`, but every register without a secret is closed. |

Any other value stops the relay at startup.

An authenticated register takes over the key from any open session holding it; the
old session is closed with `PolicyViolation` / `session_replaced` (an authenticated old
session also gets a `session_replaced` error first; an old client doesn't, so it prints
nothing). Unauthenticated (legacy) registers
keep the old `pair_key_collision` behaviour. The relay drops routed `error` envelopes
whose code is `auth_failed`, `auth_required`, `session_replaced`,
`pair_key_collision` or `rate_limited`, so peers can't forge them.

A connection acts as one key only. Re-registering the same key is fine, but a register for a
different key on a connection that already registered one is closed with `PolicyViolation` /
`one_key_per_connection` and no error message (the plugin reconnects when you switch
characters). The relay records `register_key_switch_refused` at most once per key every 5 minutes.

### Rollout order

1. Deploy the relay with `AuthMode` `Bind` on every host. Old clients keep working.
2. Ship the client that sends a secret. Each character claims its key on first connect;
   pairs, consent, wind and queued messages stay keyed by the pairing key and carry over.
3. Keep `Bind` for about 1–2 weeks, then switch to `Enforce`.

Keep the `Bind` window short. While it lasts, anyone with the shared token can claim a key
that hasn't been claimed yet (trust on first use). To spot odd claims, watch the
token-protected `/stats`: `accounts` is the total number of claimed keys, and
`accountsClaimedLast24h` is how many were claimed in the last 24 hours. A squatted key is
fixed with `reset-account` (below).

An old client whose key has been claimed is refused silently and keeps retrying. The relay
records `register_refused` (and logs it) at most once per key every 5 minutes, so these
retries don't flush the event history.

The hosts don't share a database, so a character claims its key separately on each host.

### Resetting an account

If a user loses their secret (config wiped, no copy on another PC), or someone else claimed
their key, delete their account so their next authenticated register claims the key again:

```bash
dotnet WindUpRelay.dll reset-account <PAIRINGKEY>
```

It reads `Relay:DatabasePath` the same way the server does, so run it with the same
environment as the service (for example `ASPNETCORE_ENVIRONMENT=Production`, or add
`--environment Production`). It reads the relay's config (`appsettings*.json`) from the
current directory, so run it from the relay's directory or pass `--contentRoot <dir>`. It
prints the full resolved database path before acting. `reset-account` and its key may come
anywhere on the command line, before or after such options; everything else is passed to
the configuration as usual. It may also be written `--reset-account <PAIRINGKEY>`,
`-reset-account <PAIRINGKEY>`, `/reset-account <PAIRINGKEY>` or `reset-account=<PAIRINGKEY>`.
It doesn't start the web host. Exit code `0` means the account was deleted, `1` means there
was no such account (or no database), and `2` means bad arguments: a missing or invalid key
(the key may contain only letters, digits, `-` and spaces), the command given more than once,
a bare `--`, another argument that isn't an option or an option's value, or an unknown option
whose name contains `reset`. Empty arguments are ignored. Pairs, consent, wind and the outbox
are untouched. A session that is already connected stays connected until it reconnects.

The server itself takes no such arguments either: if one is given (for example a mistyped
`reset-acount <PAIRINGKEY>`, `--reset-acount <PAIRINGKEY>` or `reset-acount=<PAIRINGKEY>`),
the relay prints usage and exits with code `2` instead of starting.

Run `reset-account` only right before the affected user reconnects. Once a key is reset,
anyone with the shared token can claim it again, so the first register with a secret wins.

## Routing between partners

The relay only carries messages between keys with an established pair (or from a key to itself,
which the plugin's debug self-pair uses):

* `wind`, `unwind`, `windRequest`, `ownerSettingsQuery`, `ownerSettingsUpdate`, `ownerGrant` and
  `call` to a key that isn't a partner are neither forwarded nor queued. The sender gets a generic
  `not_allowed` error for the request (`ownerGrant`, which has no request ID, is dropped silently).
  `pairRemove` is always delivered live, so a pair that only one side still has heals. For an offline
  key, it's queued only when it actually removed a pair or that key's own pending pair request (not
  the sender's), and only if one from the same sender isn't already waiting. `ownerRevoked` is
  delivered or queued only while the two keys are paired (the plugin sends it just before its
  `pairRemove`), and otherwise dropped silently; at most one per sender waits for an offline key.
* Replies (`presenceResult`, `windResult`, `ownerSettingsResult`, `ownerSettingsAck`, `callAck`,
  `callResult` and routed `error`s) get `from` set to the sender's own key, and are dropped unless
  the two keys are paired.
* An `ownerSettingsResult` (the only reply that can approach 64 KiB) is routed only when it was asked
  for: the relay delivered that owner's `ownerSettingsQuery` with the same request ID to this doll
  live within the last 2 minutes (kept in memory, used once), or delivered it from the outbox within
  the last 10 minutes and no result has acked that row yet. Anything else is dropped with no reply (`reply_unrequested`,
  recorded at most once per key every 5 minutes). Every other reply is routed as before: they're
  small, and many legitimately arrive long after their request.
* A `presenceQuery` for a key that isn't a partner gets the same answer as an offline key, and isn't
  forwarded.
* Queued messages are checked again when they're delivered: if the pair is gone, they're expired
  (`outbox_drop_unpaired`). Removing a pair also deletes the still-pending messages between the two
  keys, except `pairRemove` and `ownerRevoked` notices, which are still delivered. A recipient whose connection fails during a
  send, or that doesn't take a message within 5 seconds, counts as offline (`send_failed`): the
  message is queued if its type is queued, and a queued one stays pending until it was actually sent.
  A connection that doesn't take a message within 5 seconds, including the relay's own replies to
  it, is dropped, so it can't hold up its partners again and again.
* `dollStatePush` consent rows are kept only for partners (`pair_consent_unpaired` otherwise), and
  consent or wind stamps more than 5 minutes ahead of the relay's clock are rejected
  (`future_stamp_rejected`, recorded at most once per key every 5 minutes). On every start, the relay
  deletes stored consent and wind rows stamped more than 5 minutes in the future, and logs how many.
  The doll's plugin then pushes its real state again.
* The `pairSync` sent on register lists only established pairs, with consent rows only for them. A
  consent row without a pair doesn't make a partner.
* A message whose payload can't be read (wrong JSON types) is dropped (`payload_malformed`, recorded
  at most once per key every 5 minutes). The sender's connection stays open.

The plugin re-submits its partners on every connect, so pairs rebuild themselves. A fresh relay host
(for example a standby with an empty database) carries winds and other messages between two partners
only after both of them have connected to it once. Nobody needs to pair again.

**Rollout order:** deploy this relay on every host before shipping the plugin release that goes with
it, as for relay identity above. The protection against forged replies (`from`) and against
messages between keys that aren't paired lives in the relay: a new plugin on an old relay still
works, but isn't protected.

## Message and resource limits

All limits live in `WireLimits` in the shared protocol project, so relay, plugin and tests use the
same numbers. Message sizes count UTF-8 bytes per WebSocket message (all frames up to the end of
the message); field lengths count characters (UTF-16 code units) after JSON unescaping.

**Messages.** A connection may send messages of up to 4 KiB until it has registered, and up to
64 KiB after. A bigger message is closed with `1009` / `message_too_big` as soon as it crosses the
limit, without waiting for the rest of it, and a binary message with `1003` / `binary_not_supported`
at its first frame. Legitimate traffic stays far below this: the largest message an old
client sends is about 16 KB (owner settings with every emote), and a register is under 300 bytes.

**Before register.** A connection has 10 seconds to register, and may send at most 8 messages
(pings included) before it does. Otherwise it's closed with `1008` / `register_timeout` or
`1008` / `register_required`, and dropped if it doesn't answer the close within 2 seconds. A
session that has registered is never closed by the deadline, even after another session took its
key over. Re-registering on a connection counts against `Relay:MaxMessagesPerMinute` like any other
message (only pings and pongs don't).

**Fields.** Every payload the relay forwards, routes, queues or stores is checked first. Any of these
over its limit makes the whole message malformed: it's dropped with no reply (`payload_malformed`),
so nothing is echoed back. Nothing is truncated.

| Field | Limit |
|---|---|
| `requestId`, `code`, `status`, timestamps, `identity`, `worldName`, emote `name`, `remainingDisplay` | 64 |
| `message` | 256 |
| `emotes`, `dollStatePush.peers` | 512 entries |
| `to`, `from` (when given), `peerKey`, `myKey`, `theirKey` | must normalise to an 8-character key |

**Routed messages.** A message the relay builds from a client's data (a forward, a rebuilt reply, a
queued message) is dropped (`routed_too_big`) if it is over 64 KiB once re-serialised. JSON
escaping can make text up to six times bigger (`<`, `&`, `+` and non-ASCII are escaped). The relay's
own messages (`pairSync`, delivery status, relay errors) are exempt.

**Outbox.** A queued message may be at most 4 KiB. At most 100 messages from one sender to one
recipient, and 1000 for one recipient, wait at a time. When a quota is full, the new message isn't
queued (`queue_full`), and a sender whose message had a request ID gets an `error` with code
`queue_full` (old plugins show their generic failure, new ones say the partner's queue is full).
`ownerRevoked`, `pairRemove` and `pairEstablished` are exempt from the quotas, so a removal is never
lost, and at most one `ownerGrant` waits per doll and owner (a later `ownerRevoked` still deletes
it). On register, the relay delivers queued messages in order in batches of 50 until none is left or
a send fails, at most 1000 per register.

**Connections.** `Relay:MaxConnections` (default 1000, env `Relay__MaxConnections`) caps open
WebSocket connections; it's also Kestrel's `MaxConcurrentUpgradedConnections`.
`Relay:MaxConnectionsPerIp` (default 16, env `Relay__MaxConnectionsPerIp`) caps connections from one
client address (IPv6: one /64). Over either cap, the WebSocket upgrade is answered with HTTP 503.

Behind Tailscale Funnel every connection comes from tailscaled on loopback, so the client's address
can only come from a header. The relay trusts only `X-Forwarded-For` (its last entry), and only
when the TCP peer is loopback: tailscaled's serve proxy sets it to the Funnel connection's source,
replacing anything the client sent. `Tailscale-Ingress-Src` is never used, since a client can send
it itself and tailscaled doesn't strip it. A connection without a usable `X-Forwarded-For` gets no
per-IP cap at all (only the global one), so a missing header can't make everyone share one cap of
16. **The per-IP cap depends on this header, which still needs a live check through the real Funnel**
(see `plans/IN-GAME-CHECKS.md` in the plugin repo). If the relay is bound to a non-loopback address,
the TCP peer's own address is used and headers are ignored.

**First binds.** `Relay:FirstBindsPerIpPerHour` and `Relay:FirstBindsPerIpPerDay` cap how many
accounts one client address (found as for the per-IP cap: `X-Forwarded-For` from loopback, but IPv6
per /48) may create, in `Bind` and `Enforce`; 0 turns a limit off, and a missing or negative value
means 5 and 20. **The shipped `appsettings.json` sets both to 0 (off).** Turn them on, at 5 and 20,
only after the live Funnel forwarded-IP header check passes (see `plans/IN-GAME-CHECKS.md` in the
plugin repo): a header that doesn't carry each client's own address would put many users under one
limit. Only real account creations
count; registers for existing accounts, failed and secretless ones never do. A register that would
create an account over the limit creates none and isn't mapped (no legacy fallback): the connection
is closed with `1013` / `bind_rate_limited`, the plugin says the relay is limiting new accounts from
its network, and it reconnects on its normal backoff. Without the header there is no limit, as for
the per-IP cap, and none for a new address while the limiter's table (65,536 addresses) is full. The
event `bind_rate_limited` is keyed by pairing key and recorded at most once per key every 5 minutes
(`bind_race` instead when a concurrent first bind won but its account is gone on re-read, with the
same close); the address is never recorded or logged.

**Keep-alive.** The relay pings every connection every `Relay:KeepAliveIntervalSeconds` (default 30)
and drops one whose pong hasn't come within `Relay:KeepAliveTimeoutSeconds` (default 90), so a dead
peer is noticed after about two minutes instead of holding its key, routing and per-IP slot until
TCP gives up (`keepalive_timeout`, recorded at most once per key every 5 minutes). New plugins ping
the relay the same way; old plugins answer the relay's pings. An interval of 0 turns pings off
entirely, and a timeout of 0 keeps the pings but never drops a peer for a missing pong (both for
rollback).

**Closing.** The relay answers a client's close frame, so clients no longer see "closed without
completing the handshake" (a client that disposes without closing is logged at Debug only). A
session taken over by another that doesn't answer the relay's close within 2 seconds (it stopped
reading) is dropped, so it can't keep its per-IP slot; any close the relay starts is bounded the
same way.

**Events.** Every event added for these limits (`message_too_big`, `binary_message`,
`register_timeout`, `register_required`, `routed_too_big`, `queue_full`, `outbox_row_too_big`) and
`rate_limited` is recorded at most once per key every 5 minutes (once for all unregistered
connections together). An event records a message type only when it's a known one, otherwise
`unknown`.

No protocol version change: `queue_full` is the only new code a client can receive. Deploy this
relay before the plugin release that goes with it.

New plugins no longer send the shared token in the `register` body (no relay ever read it there; it
is checked only at the WebSocket upgrade, from `Authorization: Bearer` or `?token=`).
