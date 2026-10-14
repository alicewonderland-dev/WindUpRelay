# WindUpRelay hosting runbook

How to run, update, back up and rebuild the relay host. `README.md` explains what the relay does;
this file says what to do on the machine.

Never write the relay token, or any other secret, in this file or in a command you paste into a
shared place. Values in `<angle brackets>` are placeholders for the owner to fill in.

## Host

There is one relay host and no standby. Relays are no longer run locally.

| | |
|---|---|
| Machine | Oracle Cloud Always Free VM, shape `<oracle-shape>`, image `<os-image>` |
| Tailscale name | `windup-relay-oracle` (exactly this; it is compiled into every plugin) |
| Funnel URL (in the plugin's `RelayDefaults`) | `wss://windup-relay-oracle.ancon-universe.ts.net/ws` |
| SSH name | `<relay-ssh>` |
| Plugin checkout (holds this repo as `WindUpRelay/`) | `<plugin-checkout>` |
| Published releases, one folder each | `<releases-dir>` |
| Current release (a symlink to one of them; the service's working directory) | `<relay-current>` |
| Database file (absolute path, outside every release folder) | `<db>`, in `<db-dir>` |
| Service | `<service-unit>`, autostart at boot on (`<enable-autostart>`) |
| User the service runs as (owns `<db-dir>` and its files) | `<service-user>` |
| Start / stop the relay | `<start>` / `<stop>` |
| Where the relay's output goes | `<logs>` |
| Turn Funnel on / off | `<funnel-on>` / `<funnel-off>` |
| Backup copies on the VM (before they go off it) | `<local-backup-dir>` |
| Backups, off the VM | `<backup-dest>` |
| Private copy of `appsettings.Production.json`, off the VM | `<token-backup-location>` |

The service runs `dotnet WindUpRelay.dll` with `<relay-current>` as its working directory and
`ASPNETCORE_ENVIRONMENT=Production`. The relay reads its `appsettings*.json` from the working
directory, and a published release folder holds both: `dotnet publish` copies `appsettings.json` and
`appsettings.Production.json` from the checkout into it. So the copy the relay reads belongs to its
release folder: see "Changing a setting".

### Oracle Always Free

* **The shape sets the build time.** On a small shape a `dotnet publish` can take minutes. That is
  why an update publishes while the old relay keeps running, and stops it only to switch folders.
* **Oracle can reclaim an Always Free instance it considers idle** (check Oracle's current terms).
  So a rebuild is a real event, not a theory: keep the backups and the token copy off the VM, and
  see "Recovery".

### `dollhome` is retired

Every plugin build released so far lists `wss://dollhome.ancon-universe.ts.net/ws`. Release 0.2.4.0
and Testing 0.2.4.3–0.2.4.4 list it after the Oracle URL. Older builds, and Testing 0.2.4.1–0.2.4.2,
list only `dollhome` (0.2.2.0–0.2.3.5 also `dollhome-nobara`), so they can't reach the Oracle relay
at all and need an update. The next plugin release lists only the Oracle URL.

Keep the relay and Funnel on `dollhome` (and `dollhome-nobara`) off for good, or remove those nodes
from the tailnet. Don't start a relay there, not even to cover an outage: it would have its own
database, nothing syncs the two, and clients that land on it would split their accounts, pairs and
queued messages across both.

## Outages

Stopping the relay drops every session on it.

* After about 60 s with no relay (while logged in), a doll's movement and teleport locks are
  suspended until the plugin reconnects (`RelayClient.MovementLockSafetyGrace`).
* The plugin's reconnect delay doubles from 1 s up to 30 s (`RelayReconnectBackoff`), so clients
  reconnect within about 30 s of the relay coming back.

Keep every stop short. Updates build before the stop for that reason.

## Configuration

Settings are read once at startup. `appsettings.json` holds the shipped values;
`appsettings.Production.json` (gitignored, holds the token) and environment variables
(`Relay__<Setting>`, for example `Relay__MaxConnectionsPerIp`) override them.

**Changing a setting.** The relay reads the copies in its release folder, and every publish
replaces them with the checkout's. An edit to a release folder's copy is undone by the next update,
an edit to the checkout's copy does nothing until a publish, and a rollback brings back the old
release's copy. So there is one source of truth,
`<plugin-checkout>/WindUpRelay/appsettings.Production.json`:

1. Edit that file. Never edit a copy under `<releases-dir>` or `<relay-current>` by hand (the
   rollback `cp` below is the one exception: it copies this file, it doesn't edit one).
2. Refresh `<token-backup-location>` with it.
3. Publish a new release and switch to it, as in "Updating the relay", with the checkouts left at
   the deployed commits.

An environment variable in `<service-unit>` overrides the file and survives updates and rollbacks.
Use one only for a short check (as in O3), and remove it right after.

| Setting | Shipped | Notes |
|---|---|---|
| `Relay:Token` | empty | Required in Production, and the relay refuses the example placeholder. Must be the value compiled into the plugin. |
| `Relay:Urls` | `http://127.0.0.1:29417` | Keep it on loopback. `X-Forwarded-For` is trusted only from a loopback peer. |
| `Relay:DatabasePath` | empty | Empty means `windup-relay.db` next to `WindUpRelay.dll`, which would be inside a release folder, so the next update would start with an empty database. Set an absolute path (`<db>`). A relative path resolves against the working directory. |
| `Relay:AuthMode` | `Bind` | `Off`, `Bind` or `Enforce`. Any other value stops the relay at startup. |
| `Relay:MaxMessagesPerMinute` | 120 | Clamped to 10–10,000. |
| `Relay:MaxConnections` | 1000 | Global cap on open WebSockets. |
| `Relay:MaxConnectionsPerIp` | 16 | Per client address (IPv6 per /64). |
| `Relay:FirstBindsPerIpPerHour` | 0 (off) | A missing value means 5. See "Before enabling the first-bind limit". |
| `Relay:FirstBindsPerIpPerDay` | 0 (off) | A missing value means 20. |
| `Relay:KeepAliveIntervalSeconds` | 30 | 0 sends no pings (rollback only). |
| `Relay:KeepAliveTimeoutSeconds` | 90 | 0 never drops a peer for a missing pong (rollback only). |

The relay refuses to start (and exits) when the token is empty in Production, when it is still the
example placeholder, when `AuthMode` is invalid, or when the command line has a mistake it catches
(exit code `2`, see "Admin CLI"). If a start fails, read the output first.

**After every start, check the log.** The relay writes, in this order:

1. `Startup cleanup: <n> future-stamped consent/wind row(s) deleted`;
2. `WindUpKey relay listening on …`, with the listen URL, `database=`, `authMode`, the connection
   caps, `firstBindsPerIp` and `keepAlive`. `database=` is the path exactly as configured, so it
   should show the absolute `<db>`. This line is written just before the web host starts, so a
   port that is already taken shows up as an error after it;
3. ASP.NET's `Now listening on: …`, once the port is open, then `Hosting environment:` (must say
   `Production`) and `Content root path:` (the folder `<relay-current>` points at).

## Deploying and updating

### Order

1. Update the relay.
2. Only then ship the plugin release that goes with it.

Every relay change so far follows this rule (`README.md`: relay identity, routing, limits).

### Which versions work together

As far as the specs and README state it:

* **Plugins that don't send a relay secret** (every release so far: Release 0.2.4.0 and Testing
  0.2.4.4 in `repo.json`, and earlier) keep working on the current relay in `Off` and `Bind`. In
  `Bind` they work only while their key is unclaimed. In `Enforce` they are refused silently and
  keep retrying. They answer the relay's keep-alive pings. A `queue_full` refusal shows their
  generic failure message.
* **Plugins that send a secret** (from `<auth-release-version>` on) work on older relays too,
  back to the pre-auth baseline (`d7af782`): the old relay ignores the secret and never read the
  body token. But the routing protections (`from` stamping, partner-only routing) live in the
  relay, so a new plugin on an old relay isn't protected.
* No protocol version change was made for any of this. `queue_full` is the only new code a client
  can receive.

### Updating the relay

The relay builds against `../WindUpKey.Protocol` in the public plugin checkout around it (see
"Expected checkout layout" in `README.md`), so update both checkouts together. Each update is
published into a new folder while the old relay keeps running. The outage is only the stop, the
switch and the start.

```bash
# While the relay runs:
sqlite3 <db> ".backup '<local-backup-dir>/windup-relay-pre-update.db'"   # see "Backups"
# Bring <plugin-checkout> to <plugin-commit-for-this-relay>, and
# <plugin-checkout>/WindUpRelay to the relay commit you deploy: <update-method>
cd <plugin-checkout>/WindUpRelay
dotnet publish WindUpRelay.csproj -c Release -o <releases-dir>/<new-release>
test -f <releases-dir>/<new-release>/appsettings.Production.json && echo present   # don't print it
grep -o '"DatabasePath": *"[^"]*"' <releases-dir>/<new-release>/appsettings.Production.json   # must show <db>
readlink <relay-current>          # the release in use now: note it for a rollback
# From the last "WindUpKey relay listening" line in <logs>, note authMode, maxConnectionsPerIp
# and firstBindsPerIp.

# The outage starts here:
<stop>
ln -sfn <releases-dir>/<new-release> <relay-current>
<start>
```

The `grep -o` prints only the matched `"DatabasePath": "…"` pair, never the rest of the file (so
not the token). If it prints nothing, or shows an empty value or a relative path, don't stop the
relay: fix the checkout's copy (see "Changing a setting") and publish again. With an empty path the
new relay would create a fresh database inside the new release folder.

Then check the log (see "Configuration") and `/stats` (see "Logs, stats and events"). If
`database=` isn't `<db>`, roll back at once: clients are writing to a new, empty database.
`authMode`, `maxConnectionsPerIp` and `firstBindsPerIp` must match the values you noted, unless you
changed them on purpose.

**To roll back,** switch back to the folder `readlink` printed. This also brings back that
release's own `appsettings.Production.json`, so a setting changed since it was published reverts
too:

```bash
<stop>
ln -sfn <releases-dir>/<old-release> <relay-current>
<start>
```

Check the startup line again: `database=` is `<db>`, and `authMode`, `maxConnectionsPerIp` and
`firstBindsPerIp` match the values you noted. If a newer setting must stay (such as `Enforce`),
copy the checkout's file into the old release
(`cp <plugin-checkout>/WindUpRelay/appsettings.Production.json <releases-dir>/<old-release>/`),
then `<stop>`, `<start>`, and check the startup line again as above. If instead the rollback undoes a setting, set the old value in the checkout's copy too:
the next update publishes whatever it says.

Keep at least the previous release folder; delete older ones when you like.

Startup needs no migration step: tables are created with `CREATE TABLE IF NOT EXISTS`, so a new
table (such as `accounts`) appears in an existing database, and nothing is altered or dropped. The
only schema change since the baseline is the added `accounts` table, which an older relay ignores,
so a rollback keeps the same database.

### Moving to this layout (once)

The VM that runs today predates the release folders and the symlink. Move it once, in one go: the
outage is steps 2–7. `<old-relay-dir>` is the service's working directory now, and `<old-db>` is
the database it uses: the `database=` value of its startup line. Write `<old-db>` as an absolute
path (a relative one is relative to `<old-relay-dir>`). If `<old-db>` is already `<db>`, skip step 3.

1. **Note what it holds.** Take a backup as in "Backups", with `<old-db>` in place of `<db>`
   (`<db>` doesn't exist yet; a `.backup` of it would create an empty database). Ask `/stats` (see "Logs, stats and
   events") and note `accounts` and `pairs`. From the last startup line, note `database=`,
   `authMode`, `maxConnectionsPerIp` and `firstBindsPerIp`.
2. **Stop the relay** (`<stop>`).
3. **Move the database, with its `-wal` and `-shm`, to `<db>`.** Nothing may sit there yet, and
   nothing may be left behind:

   ```bash
   mkdir -p <db-dir>
   ls <db> <db>-wal <db>-shm               # each must say: No such file or directory
   for s in "" -wal -shm; do [ -e "<old-db>$s" ] && mv "<old-db>$s" "<db>$s"; done
   ls <old-db> <old-db>-wal <old-db>-shm   # each must say: No such file or directory
   sqlite3 <db> "PRAGMA integrity_check;"  # must print: ok
   chown -R <service-user> <db-dir>        # after sqlite3, which may leave -wal/-shm owned by you
   ```

4. **Set the absolute `DatabasePath`** (`<db>`) in
   `<plugin-checkout>/WindUpRelay/appsettings.Production.json`. The old relay read the copy in
   `<old-relay-dir>`, so first check that the two are the same:
   `cmp -s <old-relay-dir>/appsettings.Production.json <plugin-checkout>/WindUpRelay/appsettings.Production.json && echo same || echo differ`.
   If they differ, compare them in an editor (don't print them) and carry any hand edits into the
   checkout's copy. Then refresh `<token-backup-location>`.
5. **Publish** to `<releases-dir>/<new-release>` as in "Updating the relay", with the checkouts at
   the deployed commits, and run its `grep` check: it must show `<db>`.
6. **Create the symlink and point the service at it.** `<relay-current>` must not be a real folder
   (rename the old one first if it has that path), or `ln` puts the link inside it.

   ```bash
   ln -sfn <releases-dir>/<new-release> <relay-current>
   ```

   In `<service-unit>`, set the working directory to `<relay-current>` (it runs
   `dotnet WindUpRelay.dll` from there, as in step 10 of "Recovery"), and reload the unit if the
   service manager needs that.
7. **Start it** (`<start>`). The startup line must show `database=<db>`, and `authMode`,
   `maxConnectionsPerIp` and `firstBindsPerIp` as in step 1. `/stats` must show the `accounts` and
   `pairs` from step 1. If not, back out so the relay is up again, then find out why:
   `<stop>`; move `<db>`, `<db>-wal` and `<db>-shm` back to `<old-db>` (the same loop, the other
   way); set the working directory in `<service-unit>` back to `<old-relay-dir>`; `<start>`; and
   check that `database=` shows `<old-db>`.

Steps 4 and 5 can also be done before step 2, to shorten the outage: the old relay doesn't read
the checkout's copy. Keep `<old-relay-dir>` until all of this checks out. Roll back only to
releases published after the move: the old folder's configuration points at `<old-db>`.

## Backups

* **The database** holds all durable state: accounts (secret hashes only), pairs, consent, wind
  expiry, pending pair requests, queued messages and events. It never stores tokens, raw secrets,
  `Name@World` or ContentIds. Still, treat copies as private.
* **Backups must leave the VM.** A reclaimed or rebuilt VM takes its disk with it. Destination:
  `<backup-dest>`. Schedule: `<backup-schedule>` (daily). Keep: `<backup-retention>`.
* **Take them with SQLite's online backup.** It is safe while the relay runs, and gives one
  self-contained file. Don't copy the bare `.db` of a running relay.

  ```bash
  stamp=$(date -u +%Y%m%dT%H%M%SZ)
  sqlite3 <db> ".backup '<local-backup-dir>/windup-relay-$stamp.db'"
  sqlite3 "<local-backup-dir>/windup-relay-$stamp.db" "PRAGMA integrity_check;"   # must print: ok
  <copy-off-vm> "<local-backup-dir>/windup-relay-$stamp.db"                       # to <backup-dest>
  ```

  Also take one before every update (it can stay on the VM, for a quick rollback).
* **`appsettings.Production.json`** holds the token. Keep a private copy off the VM
  (`<token-backup-location>`), never in either repo, and refresh it after every edit (see
  "Changing a setting"). The relay's `.gitignore` already excludes it,
  and `*.db`, `*.db-wal`, `*.db-shm` and `*.log`.
* **To restore on a working VM,** check the backup as in step 1 of "Recovery". Then stop the
  relay and move the old database aside together with its `-wal` and `-shm`:

  ```bash
  <stop>
  mkdir -p <db-dir>/replaced
  for f in <db> <db>-wal <db>-shm; do [ -e "$f" ] && mv "$f" <db-dir>/replaced/; done
  ```

  A failed `mv` prints its error. Then follow steps 9 and 11 of "Recovery" (step 9's `ls` shows
  that nothing was left behind).

## Recovery: rebuilding the VM and restoring the database

Use this when the VM is gone (reclaimed or terminated) or its disk is lost. Clients can't connect
until step 12, so work through it in one go.

You need, from off the VM: the newest database backup (`<backup-dest>`), the token copy
(`<token-backup-location>`), and the plugin and relay commits that were deployed
(`<deployed-commits>`).

1. **Check the backup and note what it holds,** on any machine with `sqlite3`:

   ```bash
   sqlite3 <backup-file> "PRAGMA integrity_check;"   # must print: ok
   sqlite3 <backup-file> "SELECT (SELECT COUNT(*) FROM accounts), (SELECT COUNT(*) FROM pairs);"
   ```

2. **Delete the old `windup-relay-oracle` device in the Tailscale admin console,** before the new
   VM joins. The name is compiled into every plugin. If the old device still exists, the new one
   gets another name (such as `windup-relay-oracle-1`), and no plugin can reach it.
3. **Create the VM** (`<oracle-shape>`, `<os-image>`) and install the prerequisites: the .NET SDK
   that `global.json` in the plugin checkout asks for (`10.0.203`, with `rollForward`
   `latestFeature`), `sqlite3`, `tailscale`, and whatever `<update-method>` needs to get the code.
4. **Join the tailnet as `windup-relay-oracle`:** `sudo tailscale up --hostname=windup-relay-oracle`
   (`<tailscale-auth>`). Check in the admin console that the name is exactly
   `windup-relay-oracle`, with no suffix.
5. **Allow HTTPS and Funnel for the new node.** HTTPS certificates must be on for the tailnet, and
   the tailnet policy must give this node the `funnel` attribute (`<funnel-policy>`: by tag, user
   or device). Don't turn Funnel on yet.
6. **Lay out the checkouts** as in "Expected checkout layout" in `README.md`: the public plugin
   repo at `<plugin-checkout>`, and this repo cloned as `WindUpRelay` inside it. Check out the
   deployed commits.
7. **Restore `appsettings.Production.json`** from `<token-backup-location>` into
   `<plugin-checkout>/WindUpRelay/`. Don't make a new token: the token is compiled into every
   plugin build, and a relay with a different one answers every plugin with 401. Check that
   `Relay:DatabasePath` in it is the absolute `<db>`, and change it if the new VM keeps the
   database somewhere else.
8. **Publish the relay** into `<releases-dir>/<new-release>` and point `<relay-current>` at it, as
   in "Updating the relay" (there is no relay to stop yet).
9. **Put the database in place.** Nothing may already sit at `<db>`, `<db>-wal` or `<db>-shm`: a
   stale WAL file next to a restored database could be applied to it.

   ```bash
   mkdir -p <db-dir>
   ls <db> <db>-wal <db>-shm      # each must say: No such file or directory
   cp <backup-file> <db>
   sqlite3 <db> "PRAGMA integrity_check;"   # must print: ok
   chown -R <service-user> <db-dir>   # after sqlite3, which may leave -wal/-shm owned by you
   ```

   With no backup at all, skip this step and the relay creates an empty database. Read "Restoring
   an old backup, or none" first.
10. **Install the service** (`<service-unit>`): `dotnet WindUpRelay.dll`, working directory
    `<relay-current>`, `ASPNETCORE_ENVIRONMENT=Production`, and no bare `--` in its arguments (see
    "Admin CLI"). Turn on autostart at boot (`<enable-autostart>`).
11. **Start it** (`<start>`) and check the log (`<logs>`): `Startup cleanup:`, then
    `WindUpKey relay listening on …` with `database=` showing `<db>`, and `authMode` as before. Ask
    `/stats` (see "Logs, stats and events"): `accounts` and `pairs` must match step 1.
12. **Turn Funnel on** (`<funnel-on>`), and connect with a plugin. The log shows
    `Registered key=…`.
13. **Turn the backups back on** (`<backup-schedule>`, to `<backup-dest>`), and take one now.
14. **Keep `dollhome` off.** See "`dollhome` is retired".

### Restoring an old backup, or none

Anything that changed after the backup was taken is gone. With no backup, that is everything.
From `README.md` and the code:

* **The claim window reopens.** A key claimed after the backup has no account, so the first
  register with a secret claims it again. Anyone with the shared token (which is compiled into the
  public plugin) can be first, in `Bind` and `Enforce` alike. A plugin that sends no secret gets a
  legacy session for such a key in `Bind`. Watch `accountsClaimedLast24h` on `/stats`, and fix a
  squatted key with `reset-account`.
* **Reset keys get `auth_failed`.** A key that was reset and claimed again after the backup still
  has its old hash. That user's plugin gets `auth_failed` and stops trying until they press
  Reconnect or relog. Run `reset-account` for that key first, then ask them to press Reconnect (or
  relog). Pressing Reconnect before the reset only gets `auth_failed` again.
* **Queued messages are lost.** Winds wait up to 7 days, owner messages up to 14 days, and pair
  notices up to 30 days, so whatever was queued after the backup is gone. Consent and wind state
  come back when the doll's plugin connects and pushes its newer state. The event history goes back
  only to the backup.
* **Pairs come back by themselves.** The plugin re-submits its partners on every connect. A pair
  made after the backup carries winds and other messages again once both partners have connected.
  Nobody needs to pair again.
* **With the first-bind limit on,** every returning character whose account isn't in the restored
  database is a new account and counts against the limit. Many characters behind one address
  (alts, CGNAT, one IPv6 /48) can be refused with `bind_rate_limited` for an hour or a day; they
  retry on their own. Consider turning the limit off until they are back (see "Before enabling the
  first-bind limit").

## Rolling out AuthMode

What each mode does (details in `README.md`, "Relay identity"):

* **`Off`:** pre-auth behaviour. Secrets are ignored, and the `accounts` table isn't touched, so
  existing claims survive. Use it only to roll back.
* **`Bind`** (the default): a register with a secret claims an unclaimed key (trust on first use),
  or must match the stored hash, otherwise `auth_failed` and close. A register without a secret is
  allowed only while the key is unclaimed. For a claimed key the socket is closed with
  `PolicyViolation` / `auth_required` and no error message.
* **`Enforce`:** like `Bind`, but every register without a secret is closed. Every plugin older
  than `<auth-release-version>` can no longer connect at all. New keys can still claim on first
  use.

Steps:

1. Run the relay in `Bind`. Old clients keep working.
2. Ship the plugin release that sends a secret. Each character claims its key on its first
   connect, and its pairs, consent, wind and queued messages carry over.
3. While `Bind` lasts, watch `/stats`: `accounts` (total claimed keys) and
   `accountsClaimedLast24h`. Keep the window short, because anyone with the token can claim a key
   that hasn't been claimed yet. Fix a squatted key with `reset-account`.
4. After about 1–2 weeks, check how many keys still connect without a secret. `Enforce` cuts them
   off. Count the distinct keys with a `register_legacy` event in the last 7 days:

   ```bash
   sqlite3 -readonly <db> "SELECT COUNT(DISTINCT pairing_key) FROM relay_events WHERE kind='register_legacy' AND utc >= strftime('%Y-%m-%dT%H:%M:%S','now','-7 day');"
   sqlite3 -readonly <db> "SELECT MIN(utc) FROM relay_events;"
   ```

   `relay_events` keeps only about the newest 5,000 rows, and every register adds two. If the
   oldest row is less than 7 days old, the count covers only that span, and a key that connects
   once a week may be missing from it. The `Registered key=… auth=Legacy` log lines show the same
   thing for as far back as the log goes.
5. Set `Relay:AuthMode` to `Enforce` in the checkout's `appsettings.Production.json`, then publish
   and switch to a new release (see "Changing a setting"). The startup line shows
   `authMode=Enforce`.

To roll back, set `Bind` (or `Off`) the same way. Switching back to the previous release is
quicker, but then set `Bind` in the checkout's copy too, or the next update brings `Enforce` back.

## Admin CLI

`reset-account` deletes one key's account, so that key's next register with a secret claims it
again. Use it for a user who lost their secret, or for a squatted key. Run it on the host, from the
current release folder, with the service's environment:

```bash
cd <relay-current>
ASPNETCORE_ENVIRONMENT=Production dotnet WindUpRelay.dll reset-account <PAIRINGKEY>
# or from elsewhere:
dotnet <relay-current>/WindUpRelay.dll reset-account <PAIRINGKEY> --environment Production --contentRoot <relay-current>
```

* It reads `Relay:DatabasePath` the way the server does, and prints the full database path
  before acting. Check that it is `<db>`. It doesn't start the web host, and it never creates a
  database.
* **Exit codes:** `0` means the account was deleted; `1` means there was no such account, or no
  database; `2` means bad arguments (see `README.md` for the full list).
* Pairs, consent, wind and the outbox are untouched. A connected session stays connected until it
  reconnects.
* Run it only right before the affected user reconnects. Once a key is reset, the first register
  with a secret wins.

**A bare `--` takes the next argument (08-D1).** The relay reads its arguments the way the host
does, so `--` is an option with an empty name, and the argument after it is its value.
`-- extra` starts the server. `-- --environment Production` leaves `Production` on its own, so
the relay prints usage and exits with `2`; before 08 it silently started in Development. With
`reset-account`, a bare `--` or `/` is always refused with `2`.

**Check the service unit for a bare `--`** in the arguments that reach `WindUpRelay.dll`, and remove
it: `<service-unit>`. A start that exits with `2` fails to come up. Other mistakes are not caught: a
typo whose name doesn't contain `reset` (such as `--enviroment Production`) is passed to the
configuration, and the server starts as usual.

## Live checks through Funnel (O3)

Run these through the real Funnel URL, with a current plugin build or a scripted WebSocket client
(`<ws-test-client>`). The items come from `plans/IN-GAME-CHECKS.md` in the plugin repo (issues 01
and 03). The relay never logs the token, header values or client addresses, so don't write them
down while checking either.

- [ ] **`Authorization` header passes.** A current plugin sends the token only as
  `Authorization: Bearer`. It connects and registers through Funnel, and the relay logs
  `Client address headers: …` (written only after the token check passed) and
  `Registered key=…`. A 401 means the header didn't get through: the plugin logs
  `WindUpKey connect failed host=…`. The relay doesn't log the 401 at the shipped log levels.
- [ ] **`X-Forwarded-For` arrives.** The same log line says `X-Forwarded-For=True, per-IP cap
  applies=True` (at most one such line per 5 minutes). With `False`, the per-IP cap and the
  first-bind limit are off for that connection, and only the global cap applies.
- [ ] **It carries the client's public address,** not a Tailscale or loopback one. The value is
  never logged, so check by behaviour. Restart the relay with `Relay__MaxConnectionsPerIp=2`
  (`<set-env-and-restart>`), open three `/ws` connections from one network within the 10-second
  register window, and confirm that the third gets HTTP 503 (the relay logs
  `Refused a relay connection: ClientFull`) while a connection from another network (a phone
  hotspot, say) still gets through.
- [ ] **A client can't choose its own bucket.** Repeat the previous check with each connection
  sending a different made-up `X-Forwarded-For`: the third still gets 503. This shows the client's
  value isn't the one used. It can't tell whether tailscaled replaced the header or appended its
  own entry after the client's, because the relay reads only the last entry.
- [ ] **Trusted only from loopback.** `Relay:Urls` is still on `127.0.0.1`. From a non-loopback
  peer the relay uses the peer's own address and ignores the header. `Tailscale-Ingress-Src` is
  never read.
- [ ] **60 KiB gets through.** After registering, a 60 KiB text message (for example a `ping`
  padded with an extra field) is answered with `pong`. A 64 KiB + 1 message gets the close `1009`
  with the description `message_too_big` intact at the client.

**The cap of 2 refuses real users while it lasts.** This is the only relay, so the lowered cap
applies to everyone: any household or CGNAT address with more than two connections gets 503. Do
the two cap checks in a quiet window (`online` on `/stats` near 0), with a scripted client, and
restore the setting right after: remove the override, restart, and check that the startup line
shows the `maxConnectionsPerIp` it had before the check (16 as shipped). Each restart is a short outage (see "Outages").

`IN-GAME-CHECKS.md` also has Funnel checks for keep-alive (an idle connection stays up for
5 minutes) and for a pulled network (the session is dropped within about 2 minutes).

## Before enabling the first-bind limit (O4)

The limit caps how many accounts one client address may create (in `Bind` and `Enforce`). It
ships off (05-D8). Turn it on only once the `X-Forwarded-For` checks above pass. Otherwise a header
that doesn't carry each client's own address would put many users under one limit.

Decide first whether 5 per hour and 20 per day suit the players:

* **Per IPv6 /48, not /64.** An ISP that gives each home a /56 puts up to 256 homes in one /48;
  with a /60, up to 4,096. They all share one budget. IPv4 is per address.
* **CGNAT.** Many customers behind one IPv4 address share one budget.
* **Alts.** Each character is its own account. A player bringing many characters online for the
  first time uses one first bind each, so the sixth within an hour is refused.
* **What counts:** only real account creations. Registers for existing accounts, failed ones and
  ones without a secret never count. The counts live in memory, so they start over on a restart.
  With no header there is no limit, and none for a new address while the table (65,536 addresses)
  is full.
* **A restore from an old backup, or none,** makes every character whose account is missing a
  first bind again.
* **What a refused user sees:** the close `1013` / `bind_rate_limited`, and in the plugin "The
  relay is limiting new accounts from your network right now; retrying automatically." It
  retries on its normal backoff. No account is created.

To turn it on, set both values in the checkout's `appsettings.Production.json`, then publish and
switch to a new release (see "Changing a setting"):

```json
"Relay": {
  "FirstBindsPerIpPerHour": 5,
  "FirstBindsPerIpPerDay": 20
}
```

The startup line then shows `firstBindsPerIp=5/h,20/day`. Watch for `bind_rate_limited` events.
To turn it off again, set both to `0` the same way. Removing them falls back to `appsettings.json`,
which also says 0. Only when neither file sets them do they mean 5 and 20.

## Logs, stats and events

**Logs.** The relay writes ASP.NET's console log (stdout), at `Information`, with
`Microsoft.AspNetCore` at `Warning` (`Logging:LogLevel` in `appsettings.json`). It goes to
`<logs>`. It never logs the token, secrets, secret hashes or client addresses. Useful lines:

* `Startup cleanup: …`, and the startup line after it (see "Configuration");
* `Registered key=… auth=Bound|Authenticated|Legacy`;
* `Register refused (no secret)`, `Register auth failed`, `Register refused: too many new accounts
  from this client address`;
* `Client address headers: …` (for the Funnel checks);
* `Refused a relay connection: GlobalFull|ClientFull|KestrelUpgradeLimit` (HTTP 503 at the caps)
  and `Relay limit: <kind>`, each throttled.

**`/stats`** is protected by the token. Ask it on the host itself, in bash. The token is typed
without echo, and goes to `curl` on standard input, so it is never on a command line:

```bash
read -rs T; curl -s -H @- http://127.0.0.1:29417/stats <<<"Authorization: Bearer $T"; unset T
```

`-H @-` needs curl 7.55 or later. It returns `online`, `pairs`, `pendingSubmits`, `outboxPending`,
`outboxDelivered`, `outboxAcked`, `outboxExpired`, `eventsLast24h`, `accounts` and
`accountsClaimedLast24h`. Use the port from `Relay:Urls` if you changed it.

**Events** are kept in the database's `relay_events` table (only about the newest 5,000 rows).
Read them read-only:

```bash
sqlite3 -readonly <db> "SELECT kind, COUNT(*) FROM relay_events
  WHERE utc >= strftime('%Y-%m-%dT%H:%M:%S', 'now', '-1 day') GROUP BY kind ORDER BY 2 DESC;"
sqlite3 -readonly <db> "SELECT utc, kind, pairing_key, msg_type FROM relay_events ORDER BY id DESC LIMIT 50;"
```
