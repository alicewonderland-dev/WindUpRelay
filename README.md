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
