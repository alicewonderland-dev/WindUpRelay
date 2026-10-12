using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using WindUpKey.Protocol;

// Admin CLI: `reset-account <PAIRINGKEY>` uses the server's configuration but never starts the host.
// It is recognised anywhere in the arguments (also as --reset-account, -reset-account, /reset-account and
// reset-account=KEY); its own are stripped so the rest (e.g. --environment Production, before or after it) still
// reach the builder. The command given twice, a bare `--` with it, a missing or invalid key, or any argument the host
// can't place (a positional one, as the server takes none, or an unknown option containing "reset", so
// `reset-acount KEY` and `--reset-acount KEY` are refused too) exits 2 before the builder is even created. Blank
// arguments are ignored.
string? resetAccountKey = null;
var isResetCommand = AdminCli.TryTakeResetAccount(args, out var resetKey, out var hostArgs, out var argsError);
if (argsError is not null)
{
    Console.Error.WriteLine(argsError);
    Console.Error.WriteLine(AdminCli.Usage);
    Environment.ExitCode = 2;
    return;
}

if (isResetCommand)
{
    resetAccountKey = resetKey;
    args = hostArgs;
}

var builder = WebApplication.CreateBuilder(args);
var relayToken = builder.Configuration["Relay:Token"] ?? string.Empty;
var urls = builder.Configuration["Relay:Urls"] ?? "http://127.0.0.1:29417";
var dbPathConfig = builder.Configuration["Relay:DatabasePath"];
var dbPath = string.IsNullOrWhiteSpace(dbPathConfig)
    ? Path.Combine(AppContext.BaseDirectory, "windup-relay.db")
    : dbPathConfig;
var maxMessagesPerMinute = int.TryParse(builder.Configuration["Relay:MaxMessagesPerMinute"], out var mpm)
    ? Math.Clamp(mpm, 10, 10_000)
    : 120;

if (resetAccountKey is not null)
{
    Environment.ExitCode = AdminCli.ResetAccount(dbPath, resetAccountKey, Console.Out, Console.Error);
    return;
}

if (!RegisterAuthPolicy.TryParseMode(builder.Configuration["Relay:AuthMode"], out var authMode))
{
    throw new InvalidOperationException(
        "Relay:AuthMode must be Off, Bind or Enforce (default Bind).");
}

builder.WebHost.UseUrls(urls);

var app = builder.Build();
var log = app.Logger;

if (string.IsNullOrWhiteSpace(relayToken))
{
    if (app.Environment.IsProduction())
    {
        throw new InvalidOperationException(
            "Relay:Token is required in Production. Set it in appsettings.Production.json or env Relay__Token.");
    }

    log.LogWarning("Relay:Token is empty — OK for local dev only. Set a token before tunneling.");
}
else if (relayToken is "CHANGE_ME_TO_A_LONG_RANDOM_SECRET")
{
    throw new InvalidOperationException(
        "Replace Relay:Token in appsettings.Production.json with a long random secret before hosting.");
}

using var store = new RelayStore(dbPath);
var sessions = new SessionHub(log, store, maxMessagesPerMinute, authMode);

app.UseWebSockets();
app.MapGet("/", () => Results.Text("WindUpKey relay. Connect plugins to /ws"));
app.MapGet("/stats", (HttpContext context) =>
{
    if (!IsAuthorized(context, relayToken))
        return Results.Unauthorized();

    var online = sessions.OnlineCount;
    var stats = store.SnapshotStats();
    return Results.Json(new
    {
        online,
        pairs = stats.Pairs,
        pendingSubmits = stats.PendingSubmits,
        outboxPending = stats.OutboxPending,
        outboxDelivered = stats.OutboxDelivered,
        outboxAcked = stats.OutboxAcked,
        outboxExpired = stats.OutboxExpired,
        eventsLast24h = stats.EventsLast24h,
        accounts = stats.Accounts,
        accountsClaimedLast24h = stats.AccountsClaimedLast24h,
    });
});
app.Map("/ws", async (HttpContext context) =>
{
    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = StatusCodes.Status400BadRequest;
        return;
    }

    if (!IsAuthorized(context, relayToken))
    {
        context.Response.StatusCode = StatusCodes.Status401Unauthorized;
        return;
    }

    using var socket = await context.WebSockets.AcceptWebSocketAsync();
    await sessions.HandleClientAsync(socket, context.RequestAborted);
});

log.LogInformation("WindUpKey relay listening on {Urls}; database={Db}; authMode={AuthMode}", urls, dbPath, authMode);
app.Run();

static bool IsAuthorized(HttpContext context, string relayToken)
{
    var provided = RelayTokenAuth.GetProvidedToken(
        context.Request.Query["token"].FirstOrDefault(),
        context.Request.Headers.Authorization.FirstOrDefault());
    return RelayTokenAuth.IsAuthorized(provided, relayToken);
}

internal sealed class SessionHub(ILogger log, RelayStore store, int maxMessagesPerMinute, RelayAuthMode authMode)
{
    private static readonly TimeSpan WindOutboxTtl = TimeSpan.FromDays(7);
    private static readonly TimeSpan WindRequestCooldown = TimeSpan.FromHours(1);
    private static readonly TimeSpan OwnerOutboxTtl = TimeSpan.FromDays(14);
    private static readonly TimeSpan PairOutboxTtl = TimeSpan.FromDays(30);
    private static readonly TimeSpan RefusalRecordWindow = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan ReplacedCloseTimeout = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan CloseDrainTimeout = TimeSpan.FromSeconds(2);

    /// <summary>Close description when a connection registers a second, different key (relay-only, never an error envelope).</summary>
    internal const string OneKeyPerConnection = "one_key_per_connection";

    private readonly ConcurrentDictionary<string, ClientSession> _byPairingKey =
        new(StringComparer.Ordinal);

    private readonly PerKeyThrottle _refusalRecords = new(RefusalRecordWindow);
    private readonly PerKeyThrottle _keySwitchRecords = new(RefusalRecordWindow);
    private readonly PerKeyThrottle _authFailureRecords = new(RefusalRecordWindow);

    public int OnlineCount => _byPairingKey.Count(kv => kv.Value.IsOpen);

    public async Task HandleClientAsync(WebSocket socket, CancellationToken ct)
    {
        var session = new ClientSession(socket);
        try
        {
            var buffer = new byte[16 * 1024];
            while (socket.State == WebSocketState.Open && !ct.IsCancellationRequested)
            {
                var result = await socket.ReceiveAsync(buffer, ct);
                if (result.MessageType == WebSocketMessageType.Close)
                    break;

                if (result.MessageType != WebSocketMessageType.Text)
                    continue;

                var json = Encoding.UTF8.GetString(buffer, 0, result.Count);
                while (!result.EndOfMessage)
                {
                    result = await socket.ReceiveAsync(buffer, ct);
                    json += Encoding.UTF8.GetString(buffer, 0, result.Count);
                }

                // Once this session has been closed (refused or replaced), ignore whatever the client still sends.
                if (socket.State != WebSocketState.Open)
                    break;

                await ProcessMessageAsync(session, json, ct);
            }
        }
        catch (OperationCanceledException)
        {
            // shut down
        }
        catch (Exception ex)
        {
            log.LogWarning(ex, "WebSocket session ended with error");
        }
        finally
        {
            UnregisterSession(session);
            if (socket.State == WebSocketState.CloseSent)
            {
                // We closed first (refusal or takeover). Read until the client's close so the socket isn't
                // disposed with its queued frames unread, which can reset the connection and lose our close.
                await DrainUntilPeerCloseAsync(socket, ct);
            }
            else if (socket.State == WebSocketState.Open)
            {
                try
                {
                    await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None);
                }
                catch
                {
                    // ignore
                }
            }
        }
    }

    private static async Task DrainUntilPeerCloseAsync(WebSocket socket, CancellationToken ct)
    {
        using var drainCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        drainCts.CancelAfter(CloseDrainTimeout);
        var buffer = new byte[4 * 1024];
        try
        {
            while (socket.State == WebSocketState.CloseSent)
            {
                var result = await socket.ReceiveAsync(buffer, drainCts.Token);
                if (result.MessageType == WebSocketMessageType.Close)
                    break;
            }
        }
        catch
        {
            // Timed out or the peer is gone; the socket is disposed either way.
        }
    }

    private void UnregisterSession(ClientSession session)
    {
        if (session.PairingKey is { } key)
        {
            if (_byPairingKey.TryGetValue(key, out var current) && ReferenceEquals(current, session))
                _byPairingKey.TryRemove(key, out _);
            store.RecordEvent("unregister", pairingKey: key);
            log.LogInformation("Unregistered key={Key}", key);
        }
    }

    private async Task ProcessMessageAsync(ClientSession session, string json, CancellationToken ct)
    {
        var envelope = Envelope.TryParse(json);
        if (envelope is null)
            return;

        if (session.PairingKey is not null
            && envelope.Type is not (MessageTypes.Ping or MessageTypes.Pong or MessageTypes.Register)
            && !store.TryConsumeRate(session.PairingKey, maxMessagesPerMinute))
        {
            store.RecordEvent("rate_limited", pairingKey: session.PairingKey, msgType: envelope.Type);
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                Code = ErrorCodes.RateLimited,
                Message = "Too many messages; try again shortly.",
            }), ct);
            return;
        }

        switch (envelope.Type)
        {
            case MessageTypes.Register:
                await HandleRegisterAsync(session, envelope, ct);
                break;
            case MessageTypes.Wind:
                await HandleWindAsync(session, envelope, ct);
                break;
            case MessageTypes.WindRequest:
                await HandleWindRequestAsync(session, envelope, ct);
                break;
            case MessageTypes.Unwind:
                await HandleUnwindAsync(session, envelope, ct);
                break;
            case MessageTypes.OwnerGrant:
                await HandleOwnerGrantAsync(session, envelope, ct);
                break;
            case MessageTypes.OwnerRevoked:
                await HandleOwnerRevokedAsync(session, envelope, ct);
                break;
            case MessageTypes.OwnerSettingsQuery:
                await HandleOwnerSettingsQueryAsync(session, envelope, ct);
                break;
            case MessageTypes.OwnerSettingsUpdate:
                await HandleOwnerSettingsUpdateAsync(session, envelope, ct);
                break;
            case MessageTypes.Call:
                await HandleCallAsync(session, envelope, ct);
                break;
            case MessageTypes.PairSubmit:
                await HandlePairSubmitAsync(session, envelope, ct);
                break;
            case MessageTypes.PairCancel:
                HandlePairCancel(session, envelope);
                break;
            case MessageTypes.PairRemove:
                await HandlePairRemoveAsync(session, envelope, ct);
                break;
            case MessageTypes.DollStatePush:
                HandleDollStatePush(session, envelope);
                break;
            case MessageTypes.PresenceQuery:
                await HandlePresenceQueryAsync(session, envelope, ct);
                break;
            case MessageTypes.PresenceResult:
            case MessageTypes.WindResult:
            case MessageTypes.OwnerSettingsResult:
            case MessageTypes.OwnerSettingsAck:
            case MessageTypes.CallAck:
            case MessageTypes.CallResult:
            case MessageTypes.Error:
                await RouteByKeyAsync(envelope, ct);
                break;
            case MessageTypes.Ping:
                await session.SendAsync(Envelope.Create(MessageTypes.Pong, new { }), ct);
                break;
            case MessageTypes.Pong:
                break;
            default:
                break;
        }
    }

    private async Task HandleRegisterAsync(ClientSession session, Envelope envelope, CancellationToken ct)
    {
        var payload = envelope.GetPayload<RegisterPayload>();
        var pairingKey = NormalizePairingKey(payload?.PairingKey);
        if (pairingKey.Length != 8)
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                Code = ErrorCodes.BadRequest,
                Message = "register requires pairingKey",
            }), ct);
            return;
        }

        // One key per connection: switching characters reconnects, so a second key here is never legitimate.
        // Checked before the auth decision so it can't claim or probe a second account.
        if (session.RegisteredKey is { } registeredKey
            && !string.Equals(registeredKey, pairingKey, StringComparison.Ordinal))
        {
            if (_keySwitchRecords.TryAcquire(registeredKey))
            {
                store.RecordEvent("register_key_switch_refused", pairingKey: registeredKey);
                log.LogInformation("Register for a second key refused on connection of key={Key}", registeredKey);
            }

            await session.CloseAsync(WebSocketCloseStatus.PolicyViolation, OneKeyPerConnection, ct);
            return;
        }

        var outcome = DecideRegisterAuth(pairingKey, payload?.Secret);
        if (outcome == RegisterAuthOutcome.RefuseSilently)
        {
            // No error envelope: old clients print a wind failure for unknown codes. They also retry every
            // second or two, so record and log this at most once per key per window.
            if (_refusalRecords.TryAcquire(pairingKey))
            {
                store.RecordEvent("register_refused", pairingKey: pairingKey);
                log.LogInformation("Register refused (no secret) key={Key}", pairingKey);
            }

            await session.CloseAsync(WebSocketCloseStatus.PolicyViolation, ErrorCodes.AuthRequired, ct);
            return;
        }

        if (outcome == RegisterAuthOutcome.AuthFailed)
        {
            // A client with the wrong secret may keep retrying: record and log at most once per key per window.
            if (_authFailureRecords.TryAcquire(pairingKey))
            {
                store.RecordEvent("register_auth_failed", pairingKey: pairingKey);
                log.LogInformation("Register auth failed key={Key}", pairingKey);
            }

            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                Code = ErrorCodes.AuthFailed,
                Message = "relay identity does not match this pairing key",
            }), ct);
            await session.CloseAsync(WebSocketCloseStatus.PolicyViolation, ErrorCodes.AuthFailed, ct);
            return;
        }

        var authenticated = outcome is RegisterAuthOutcome.Bound or RegisterAuthOutcome.Authenticated;

        // Take the identity before the map points here: a concurrent takeover that displaces this session
        // strips it afterwards, so a displaced session never keeps acting as this key. Both the set and the
        // restore are compare-exchanges, so neither can undo a strip that happened in between.
        var previous = session.Identity;
        var mine = new SessionIdentity(pairingKey, authenticated);
        if (!session.TrySwapIdentity(previous, mine))
            return; // Displaced meanwhile; this session is being closed.

        // Authenticated sessions take over the key from whoever holds it; legacy ones get pair_key_collision.
        if (!SessionMap.TryClaim(_byPairingKey, pairingKey, session, authenticated, s => s.IsOpen, out var replaced))
        {
            session.TrySwapIdentity(mine, previous);
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                Code = ErrorCodes.PairKeyCollision,
                Message = "pairing key in use",
            }), ct);
            return;
        }

        // Strip the displaced session before anything else (DB writes, logging) so it stops acting as the key.
        if (replaced is not null)
            ReplaceSession(replaced, pairingKey);

        session.MarkRegistered(pairingKey);
        store.RecordEvent("register", pairingKey: pairingKey);
        if (authMode != RelayAuthMode.Off)
        {
            store.RecordEvent(outcome switch
            {
                RegisterAuthOutcome.Bound => "register_bound",
                RegisterAuthOutcome.Authenticated => "register_auth_ok",
                _ => "register_legacy",
            }, pairingKey: pairingKey);
        }

        log.LogInformation("Registered key={Key} auth={Outcome}", pairingKey, outcome);

        var peers = store.GetPeerKeys(pairingKey);
        var consent = store.GetPairConsentForDoll(pairingKey);
        var wind = store.GetDollWind(pairingKey);
        var peerKeys = peers.Count > 0
            ? peers.ToList()
            : consent.Select(c => c.PeerKey).Distinct(StringComparer.Ordinal).OrderBy(k => k).ToList();

        await session.SendAsync(Envelope.Create(MessageTypes.PairSync, new PairSyncPayload
        {
            PeerKeys = peerKeys,
            Peers = consent.Select(c => new PairConsentSyncEntry
            {
                PeerKey = c.PeerKey,
                CanWindMe = c.CanWind,
                CanUnwindMe = c.CanUnwind,
                IsOwner = c.IsOwner,
                CanCallMe = c.CanCall,
                UpdatedUtc = c.UpdatedUtc,
            }).ToList(),
            ExpiryUtc = wind?.ExpiryUtc,
            WindUpdatedUtc = wind?.UpdatedUtc,
        }), ct);

        await TryCompletePendingForAsync(session, ct);
        await FlushOutboxAsync(session, ct);
    }

    private RegisterAuthOutcome DecideRegisterAuth(string pairingKey, string? secret)
    {
        var outcome = RegisterAuthPolicy.Resolve(
            authMode,
            secret,
            () => store.GetAccountSecretHash(pairingKey),
            hash => store.TryCreateAccount(pairingKey, hash));
        if (outcome == RegisterAuthOutcome.Authenticated)
            store.TouchAccount(pairingKey);
        return outcome;
    }

    /// <summary>
    /// Called after the new session is already mapped. Strips the old session's identity right away, then
    /// tells it and closes it in the background with a short timeout, so the new session never waits on it.
    /// </summary>
    private void ReplaceSession(ClientSession replaced, string pairingKey)
    {
        // Old clients print a generic error for unknown codes, so only authenticated (new) clients get the
        // envelope. Everyone gets the PolicyViolation/session_replaced close.
        var notify = replaced.Authenticated;

        // Strip identity first so nothing still in flight on the old socket acts as this key.
        replaced.Identity = null;
        _ = CloseReplacedSessionAsync(replaced, pairingKey, notify);
        store.RecordEvent("session_replaced", pairingKey: pairingKey);
        log.LogInformation("Session replaced key={Key}", pairingKey);
    }

    private async Task CloseReplacedSessionAsync(ClientSession replaced, string pairingKey, bool notify)
    {
        // Relay-originated, so no `to`: that's how clients tell it from a routed error.
        using var timeout = new CancellationTokenSource(ReplacedCloseTimeout);
        try
        {
            if (notify)
            {
                await replaced.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
                {
                    Code = ErrorCodes.SessionReplaced,
                    Message = "another session for this pairing key took over",
                }), timeout.Token);
            }

            await replaced.CloseAsync(WebSocketCloseStatus.PolicyViolation, ErrorCodes.SessionReplaced, timeout.Token);
        }
        catch (Exception ex)
        {
            // A stuck or broken old socket is aborted, so its own loop ends too.
            log.LogDebug(ex, "Closing replaced session failed key={Key}", pairingKey);
            replaced.Abort();
        }
    }

    private void HandleDollStatePush(ClientSession session, Envelope envelope)
    {
        if (session.PairingKey is not { } dollKey)
            return;

        var payload = envelope.GetPayload<DollStatePushPayload>();
        if (payload is null)
            return;

        if (!string.IsNullOrEmpty(payload.WindUpdatedUtc))
        {
            string? expiryUtc = payload.ClearWind ? null : payload.ExpiryUtc;
            if (store.TryMergeDollWind(dollKey, expiryUtc, payload.WindUpdatedUtc))
            {
                store.RecordEvent(
                    "doll_wind_merge",
                    pairingKey: dollKey,
                    msgType: MessageTypes.DollStatePush,
                    detail: payload.ClearWind ? "clear" : "set");
            }
        }

        if (payload.Peers is null || payload.Peers.Count == 0)
            return;

        foreach (var peer in payload.Peers)
        {
            var peerKey = NormalizePairingKey(peer.PeerKey);
            if (peerKey.Length != 8 || string.Equals(peerKey, dollKey, StringComparison.Ordinal))
                continue;

            if (store.TryMergePairConsent(new PairConsentRow(
                    dollKey,
                    peerKey,
                    peer.CanWindMe,
                    peer.CanUnwindMe,
                    peer.IsOwner,
                    peer.CanCallMe,
                    peer.UpdatedUtc)))
            {
                store.RecordEvent(
                    "pair_consent_merge",
                    pairingKey: dollKey,
                    peerKey: peerKey,
                    msgType: MessageTypes.DollStatePush);
            }
        }
    }

    private async Task HandlePairSubmitAsync(ClientSession session, Envelope envelope, CancellationToken ct)
    {
        if (session.PairingKey is null)
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                Code = ErrorCodes.NotRegistered,
                Message = "Pairing could not be completed.",
            }), ct);
            return;
        }

        var payload = envelope.GetPayload<PairSubmitPayload>();
        if (payload is null)
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                Code = ErrorCodes.PairFailed,
                Message = "Pairing could not be completed.",
            }), ct);
            return;
        }

        var myKey = NormalizePairingKey(payload.MyKey);
        var theirKey = NormalizePairingKey(payload.TheirKey);
        if (myKey.Length != 8 || theirKey.Length != 8 || myKey == theirKey
            || !string.Equals(myKey, session.PairingKey, StringComparison.Ordinal))
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                RequestId = payload.RequestId,
                Code = ErrorCodes.PairFailed,
                Message = "Pairing could not be completed.",
            }), ct);
            return;
        }

        store.UpsertPairSubmit(myKey, theirKey);
        store.RecordEvent("pair_submit", pairingKey: myKey, peerKey: theirKey);

        if (store.HasPairSubmit(theirKey, myKey))
            await EstablishPairAsync(myKey, theirKey, ct);
    }

    private async Task TryCompletePendingForAsync(ClientSession session, CancellationToken ct)
    {
        if (session.PairingKey is null)
            return;

        foreach (var (myKey, theirKey) in store.GetMutualPendingFor(session.PairingKey))
            await EstablishPairAsync(myKey, theirKey, ct);
    }

    private void HandlePairCancel(ClientSession session, Envelope envelope)
    {
        if (session.PairingKey is null)
            return;

        var payload = envelope.GetPayload<PairCancelPayload>();
        var peerKey = NormalizePairingKey(payload?.PeerKey);
        if (peerKey.Length != 8 || string.Equals(peerKey, session.PairingKey, StringComparison.Ordinal))
            return;

        store.CancelPairSubmit(session.PairingKey, peerKey);
        store.RecordEvent("pair_cancel", pairingKey: session.PairingKey, peerKey: peerKey);
    }

    private async Task EstablishPairAsync(string a, string b, CancellationToken ct)
    {
        if (!store.TryEstablishPair(a, b))
            return;

        store.RecordEvent("pair_established", pairingKey: a, peerKey: b);
        log.LogInformation("Pair established {A} <-> {B}", a, b);

        await DeliverOrQueuePairEstablishedAsync(a, b, ct);
        await DeliverOrQueuePairEstablishedAsync(b, a, ct);
    }

    private async Task DeliverOrQueuePairEstablishedAsync(string toKey, string peerKey, CancellationToken ct)
    {
        var envelope = Envelope.Create(MessageTypes.PairEstablished, new PairEstablishedPayload
        {
            PeerKey = peerKey,
        });

        if (_byPairingKey.TryGetValue(toKey, out var session) && session.IsOpen)
        {
            await session.SendAsync(envelope, ct);
            return;
        }

        store.EnqueueOutbox(null, peerKey, toKey, MessageTypes.PairEstablished, envelope.Serialize(), PairOutboxTtl);
        store.RecordEvent("queue", pairingKey: peerKey, peerKey: toKey, msgType: MessageTypes.PairEstablished);
    }

    private async Task HandlePairRemoveAsync(ClientSession session, Envelope envelope, CancellationToken ct)
    {
        var payload = envelope.GetPayload<PairRemovePayload>();
        if (payload is null || session.PairingKey is null)
            return;

        var peerKey = NormalizePairingKey(payload.PeerKey);
        if (peerKey.Length != 8)
            return;

        store.RemovePair(session.PairingKey, peerKey);
        store.RecordEvent("pair_remove", pairingKey: session.PairingKey, peerKey: peerKey);

        var notify = Envelope.Create(MessageTypes.PairRemove, new PairRemovePayload
        {
            PeerKey = session.PairingKey,
        });

        if (_byPairingKey.TryGetValue(peerKey, out var peer) && peer.IsOpen)
        {
            await peer.SendAsync(notify, ct);
            return;
        }

        store.EnqueueOutbox(null, session.PairingKey, peerKey, MessageTypes.PairRemove, notify.Serialize(), PairOutboxTtl);
    }

    private async Task HandlePresenceQueryAsync(ClientSession session, Envelope envelope, CancellationToken ct)
    {
        if (session.PairingKey is null)
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                Code = ErrorCodes.NotRegistered,
                Message = "register before presence query",
            }), ct);
            return;
        }

        var payload = envelope.GetPayload<PresenceQueryPayload>();
        if (payload is null || string.IsNullOrWhiteSpace(payload.To))
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                RequestId = payload?.RequestId,
                Code = ErrorCodes.BadRequest,
                Message = "invalid presence query",
            }), ct);
            return;
        }

        payload.From = session.PairingKey;
        payload.To = NormalizePairingKey(payload.To);

        if (!_byPairingKey.TryGetValue(payload.To, out var target) || !target.IsOpen)
        {
            await session.SendAsync(Envelope.Create(MessageTypes.PresenceResult, new PresenceResultPayload
            {
                RequestId = payload.RequestId,
                From = payload.To,
                To = session.PairingKey,
                Online = false,
            }), ct);
            return;
        }

        await session.SendAsync(Envelope.Create(MessageTypes.PresenceResult, new PresenceResultPayload
        {
            RequestId = payload.RequestId,
            From = payload.To,
            To = session.PairingKey,
            Online = true,
        }), ct);

        await target.SendAsync(Envelope.Create(MessageTypes.PresenceQuery, payload), ct);
    }

    private async Task HandleWindAsync(ClientSession session, Envelope envelope, CancellationToken ct)
    {
        if (session.PairingKey is null)
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                Code = ErrorCodes.NotRegistered,
                Message = "register before sending wind",
            }), ct);
            return;
        }

        var payload = envelope.GetPayload<WindPayload>();
        if (payload is null || string.IsNullOrWhiteSpace(payload.To) || payload.Hours <= 0)
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                RequestId = payload?.RequestId,
                Code = ErrorCodes.BadRequest,
                Message = "invalid wind payload",
            }), ct);
            return;
        }

        payload.From = session.PairingKey;
        payload.To = NormalizePairingKey(payload.To);
        var forward = Envelope.Create(MessageTypes.Wind, payload);
        await DeliverOrQueueAsync(session, payload.RequestId, payload.To, MessageTypes.Wind, forward, WindOutboxTtl, ct);
    }

    private async Task HandleUnwindAsync(ClientSession session, Envelope envelope, CancellationToken ct)
    {
        if (session.PairingKey is null)
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                Code = ErrorCodes.NotRegistered,
                Message = "register before sending unwind",
            }), ct);
            return;
        }

        var payload = envelope.GetPayload<UnwindPayload>();
        if (payload is null || string.IsNullOrWhiteSpace(payload.To))
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                RequestId = payload?.RequestId,
                Code = ErrorCodes.BadRequest,
                Message = "invalid unwind payload",
            }), ct);
            return;
        }

        payload.From = session.PairingKey;
        payload.To = NormalizePairingKey(payload.To);
        var forward = Envelope.Create(MessageTypes.Unwind, payload);
        await DeliverOrQueueAsync(session, payload.RequestId, payload.To, MessageTypes.Unwind, forward, WindOutboxTtl, ct);
    }

    private async Task HandleWindRequestAsync(ClientSession session, Envelope envelope, CancellationToken ct)
    {
        if (session.PairingKey is null)
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                Code = ErrorCodes.NotRegistered,
                Message = "register before sending windRequest",
            }), ct);
            return;
        }

        var payload = envelope.GetPayload<WindRequestPayload>();
        if (payload is null
            || string.IsNullOrWhiteSpace(payload.RequestId)
            || string.IsNullOrWhiteSpace(payload.To)
            || payload.PercentRemaining is < 0 or > 100)
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                RequestId = payload?.RequestId,
                Code = ErrorCodes.BadRequest,
                Message = "invalid windRequest payload",
            }), ct);
            return;
        }

        payload.From = session.PairingKey;
        payload.To = NormalizePairingKey(payload.To);
        if (!store.HasEstablishedPair(session.PairingKey, payload.To))
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                RequestId = payload.RequestId,
                Code = ErrorCodes.NotAllowed,
                Message = "wind requests require an established pair",
            }), ct);
            return;
        }

        if (!store.TryConsumeWindRequest(session.PairingKey, WindRequestCooldown))
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                RequestId = payload.RequestId,
                Code = ErrorCodes.WindRequestCooldown,
                Message = "wind requests are limited to one per hour",
            }), ct);
            return;
        }

        var forward = Envelope.Create(MessageTypes.WindRequest, payload);
        await DeliverOrQueueAsync(
            session,
            payload.RequestId,
            payload.To,
            MessageTypes.WindRequest,
            forward,
            WindOutboxTtl,
            ct);
    }

    private async Task HandleOwnerGrantAsync(ClientSession session, Envelope envelope, CancellationToken ct)
    {
        if (session.PairingKey is null)
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                Code = ErrorCodes.NotRegistered,
                Message = "register before sending ownerGrant",
            }), ct);
            return;
        }

        var payload = envelope.GetPayload<OwnerGrantPayload>();
        if (payload is null || string.IsNullOrWhiteSpace(payload.To))
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                Code = ErrorCodes.BadRequest,
                Message = "invalid ownerGrant payload",
            }), ct);
            return;
        }

        // Never persist optional identity labels in the outbox — strip before queue/forward.
        payload.From = session.PairingKey;
        payload.To = NormalizePairingKey(payload.To);
        var identity = payload.Identity;
        var live = Envelope.Create(MessageTypes.OwnerGrant, payload);
        if (_byPairingKey.TryGetValue(payload.To, out var target) && target.IsOpen)
        {
            await target.SendAsync(live, ct);
            return;
        }

        payload.Identity = null;
        var queued = Envelope.Create(MessageTypes.OwnerGrant, payload);
        store.EnqueueOutbox(null, session.PairingKey, payload.To, MessageTypes.OwnerGrant, queued.Serialize(), OwnerOutboxTtl);
        store.RecordEvent("queue", pairingKey: session.PairingKey, peerKey: payload.To, msgType: MessageTypes.OwnerGrant);
        _ = identity; // local label is live-only; offline recipients restore without Name@World
    }

    private async Task HandleOwnerRevokedAsync(ClientSession session, Envelope envelope, CancellationToken ct)
    {
        if (session.PairingKey is null)
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                Code = ErrorCodes.NotRegistered,
                Message = "register before sending ownerRevoked",
            }), ct);
            return;
        }

        var payload = envelope.GetPayload<OwnerRevokedPayload>();
        if (payload is null || string.IsNullOrWhiteSpace(payload.To))
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                Code = ErrorCodes.BadRequest,
                Message = "invalid ownerRevoked payload",
            }), ct);
            return;
        }

        payload.From = session.PairingKey;
        payload.To = NormalizePairingKey(payload.To);
        var forward = Envelope.Create(MessageTypes.OwnerRevoked, payload);
        await DeliverOrQueueSilentAsync(session.PairingKey, payload.To, MessageTypes.OwnerRevoked, forward, OwnerOutboxTtl, ct);
    }

    private async Task HandleOwnerSettingsQueryAsync(ClientSession session, Envelope envelope, CancellationToken ct)
    {
        if (session.PairingKey is null)
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                Code = ErrorCodes.NotRegistered,
                Message = "register before sending ownerSettingsQuery",
            }), ct);
            return;
        }

        var payload = envelope.GetPayload<OwnerSettingsQueryPayload>();
        if (payload is null || string.IsNullOrWhiteSpace(payload.To))
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                RequestId = payload?.RequestId,
                Code = ErrorCodes.BadRequest,
                Message = "invalid ownerSettingsQuery payload",
            }), ct);
            return;
        }

        payload.From = session.PairingKey;
        payload.To = NormalizePairingKey(payload.To);
        var forward = Envelope.Create(MessageTypes.OwnerSettingsQuery, payload);
        await DeliverOrQueueAsync(session, payload.RequestId, payload.To, MessageTypes.OwnerSettingsQuery, forward, OwnerOutboxTtl, ct);
    }

    private async Task HandleOwnerSettingsUpdateAsync(ClientSession session, Envelope envelope, CancellationToken ct)
    {
        if (session.PairingKey is null)
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                Code = ErrorCodes.NotRegistered,
                Message = "register before sending ownerSettingsUpdate",
            }), ct);
            return;
        }

        var payload = envelope.GetPayload<OwnerSettingsUpdatePayload>();
        if (payload is null || string.IsNullOrWhiteSpace(payload.To))
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                RequestId = payload?.RequestId,
                Code = ErrorCodes.BadRequest,
                Message = "invalid ownerSettingsUpdate payload",
            }), ct);
            return;
        }

        payload.From = session.PairingKey;
        payload.To = NormalizePairingKey(payload.To);
        var forward = Envelope.Create(MessageTypes.OwnerSettingsUpdate, payload);
        await DeliverOrQueueAsync(session, payload.RequestId, payload.To, MessageTypes.OwnerSettingsUpdate, forward, OwnerOutboxTtl, ct);
    }

    /// <summary>Live-only: never queue stale summons.</summary>
    private async Task HandleCallAsync(ClientSession session, Envelope envelope, CancellationToken ct)
    {
        if (session.PairingKey is null)
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                Code = ErrorCodes.NotRegistered,
                Message = "register before sending call",
            }), ct);
            return;
        }

        var payload = envelope.GetPayload<CallPayload>();
        if (payload is null || string.IsNullOrWhiteSpace(payload.To) || payload.WorldId == 0 || payload.TerritoryId == 0)
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                RequestId = payload?.RequestId,
                Code = ErrorCodes.BadRequest,
                Message = "invalid call payload",
            }), ct);
            return;
        }

        payload.From = session.PairingKey;
        payload.To = NormalizePairingKey(payload.To);
        var forward = Envelope.Create(MessageTypes.Call, payload);

        if (_byPairingKey.TryGetValue(payload.To, out var target) && target.IsOpen)
        {
            await target.SendAsync(forward, ct);
            return;
        }

        await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
        {
            RequestId = payload.RequestId,
            To = session.PairingKey,
            Code = ErrorCodes.TargetOffline,
            Message = "doll is offline",
        }), ct);
    }

    private async Task DeliverOrQueueAsync(
        ClientSession sender,
        string? requestId,
        string toKey,
        string msgType,
        Envelope forward,
        TimeSpan ttl,
        CancellationToken ct)
    {
        if (_byPairingKey.TryGetValue(toKey, out var target) && target.IsOpen)
        {
            await target.SendAsync(forward, ct);
            return;
        }

        if (sender.PairingKey is null)
            return;

        store.EnqueueOutbox(requestId, sender.PairingKey, toKey, msgType, forward.Serialize(), ttl);
        store.RecordEvent("queue", pairingKey: sender.PairingKey, peerKey: toKey, msgType: msgType);

        if (!string.IsNullOrEmpty(requestId))
        {
            await sender.SendAsync(Envelope.Create(MessageTypes.DeliveryQueued, new DeliveryStatusPayload
            {
                RequestId = requestId,
                To = toKey,
                Type = msgType,
            }), ct);
        }
    }

    private async Task DeliverOrQueueSilentAsync(
        string fromKey,
        string toKey,
        string msgType,
        Envelope forward,
        TimeSpan ttl,
        CancellationToken ct)
    {
        if (_byPairingKey.TryGetValue(toKey, out var target) && target.IsOpen)
        {
            await target.SendAsync(forward, ct);
            return;
        }

        store.EnqueueOutbox(null, fromKey, toKey, msgType, forward.Serialize(), ttl);
        store.RecordEvent("queue", pairingKey: fromKey, peerKey: toKey, msgType: msgType);
        await Task.CompletedTask;
    }

    private async Task FlushOutboxAsync(ClientSession session, CancellationToken ct)
    {
        if (session.PairingKey is null)
            return;

        store.ExpirePending();
        foreach (var row in store.TakePendingFor(session.PairingKey))
        {
            var envelope = Envelope.TryParse(row.EnvelopeJson);
            if (envelope is null)
            {
                store.MarkDelivered(row.Id);
                continue;
            }

            await session.SendAsync(envelope, ct);
            store.MarkDelivered(row.Id);
            store.RecordEvent("deliver", pairingKey: row.FromKey, peerKey: session.PairingKey, msgType: row.MsgType);

            if (string.IsNullOrEmpty(row.RequestId))
                continue;

            if (_byPairingKey.TryGetValue(row.FromKey, out var sender) && sender.IsOpen)
            {
                await sender.SendAsync(Envelope.Create(MessageTypes.DeliveryDelivered, new DeliveryStatusPayload
                {
                    RequestId = row.RequestId,
                    To = session.PairingKey,
                    Type = row.MsgType,
                }), ct);
            }
        }
    }

    private async Task RouteByKeyAsync(Envelope envelope, CancellationToken ct)
    {
        // Relay-only session codes can't be forged by addressing an error to another key.
        if (envelope.Type == MessageTypes.Error
            && ErrorCodes.IsRelayOnly(envelope.GetPayload<ErrorPayload>()?.Code))
            return;

        string? to = envelope.Type switch
        {
            MessageTypes.WindResult => NormalizePairingKey(envelope.GetPayload<WindResultPayload>()?.To),
            MessageTypes.PresenceResult => NormalizePairingKey(envelope.GetPayload<PresenceResultPayload>()?.To),
            MessageTypes.OwnerSettingsResult => NormalizePairingKey(envelope.GetPayload<OwnerSettingsResultPayload>()?.To),
            MessageTypes.OwnerSettingsAck => NormalizePairingKey(envelope.GetPayload<OwnerSettingsAckPayload>()?.To),
            MessageTypes.CallAck => NormalizePairingKey(envelope.GetPayload<CallAckPayload>()?.To),
            MessageTypes.CallResult => NormalizePairingKey(envelope.GetPayload<CallResultPayload>()?.To),
            MessageTypes.Error => NormalizePairingKey(envelope.GetPayload<ErrorPayload>()?.To),
            _ => null,
        };

        if (envelope.Type == MessageTypes.WindResult)
        {
            var rid = envelope.GetPayload<WindResultPayload>()?.RequestId;
            if (!string.IsNullOrEmpty(rid))
                store.MarkAckedByRequestId(rid);
        }
        else if (envelope.Type is MessageTypes.OwnerSettingsResult or MessageTypes.OwnerSettingsAck)
        {
            var rid = envelope.Type == MessageTypes.OwnerSettingsResult
                ? envelope.GetPayload<OwnerSettingsResultPayload>()?.RequestId
                : envelope.GetPayload<OwnerSettingsAckPayload>()?.RequestId;
            if (!string.IsNullOrEmpty(rid))
                store.MarkAckedByRequestId(rid);
        }
        else if (envelope.Type is MessageTypes.CallAck or MessageTypes.CallResult)
        {
            var rid = envelope.Type == MessageTypes.CallAck
                ? envelope.GetPayload<CallAckPayload>()?.RequestId
                : envelope.GetPayload<CallResultPayload>()?.RequestId;
            if (!string.IsNullOrEmpty(rid))
                store.MarkAckedByRequestId(rid);
        }

        if (string.IsNullOrWhiteSpace(to) || to.Length != 8)
            return;

        if (!_byPairingKey.TryGetValue(to, out var dest) || !dest.IsOpen)
            return;

        await dest.SendAsync(envelope, ct);
    }

    internal static string NormalizePairingKey(string? key)
    {
        if (string.IsNullOrWhiteSpace(key))
            return string.Empty;
        var chars = key.Trim().ToUpperInvariant().Where(char.IsAsciiLetterOrDigit).ToArray();
        return new string(chars);
    }
}

internal sealed class ClientSession(WebSocket socket)
{
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    // Read and written from this session's loop and from other sessions' loops (takeover strips it),
    // so key and auth flag live in one immutable object that is swapped atomically.
    private SessionIdentity? _identity;
    private string? _registeredKey;

    /// <summary>The key this session acts as, or null before register or after it was displaced.</summary>
    public SessionIdentity? Identity
    {
        get => Volatile.Read(ref _identity);
        set => Volatile.Write(ref _identity, value);
    }

    public string? PairingKey => Identity?.PairingKey;

    /// <summary>True when the last register proved ownership of <see cref="PairingKey"/> with its secret.</summary>
    public bool Authenticated => Identity?.Authenticated ?? false;

    /// <summary>
    /// The first key this connection registered successfully. A connection acts as one key only, so it never
    /// changes once set, even after the session is displaced.
    /// </summary>
    public string? RegisteredKey => Volatile.Read(ref _registeredKey);

    /// <summary>Sets <see cref="Identity"/> to <paramref name="value"/> only while it is still <paramref name="expected"/>.</summary>
    public bool TrySwapIdentity(SessionIdentity? expected, SessionIdentity? value) =>
        ReferenceEquals(Interlocked.CompareExchange(ref _identity, value, expected), expected);

    public void MarkRegistered(string pairingKey) => Interlocked.CompareExchange(ref _registeredKey, pairingKey, null);

    public bool IsOpen => socket.State == WebSocketState.Open;

    /// <summary>Drops the connection without a close handshake (for a socket that is stuck).</summary>
    public void Abort() => socket.Abort();

    public async Task SendAsync(Envelope envelope, CancellationToken ct)
    {
        var bytes = Encoding.UTF8.GetBytes(envelope.Serialize());
        await _sendLock.WaitAsync(ct);
        try
        {
            if (socket.State != WebSocketState.Open)
                return;
            await socket.SendAsync(bytes, WebSocketMessageType.Text, true, ct);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    /// <summary>
    /// Sends a close frame without waiting for the peer's reply, so it is safe while this
    /// session's own loop is blocked in ReceiveAsync. Once the socket has left Open, that loop
    /// stops processing frames and drains until the peer's close (bounded) before disposing.
    /// </summary>
    public async Task CloseAsync(WebSocketCloseStatus status, string description, CancellationToken ct)
    {
        await _sendLock.WaitAsync(ct);
        try
        {
            if (socket.State != WebSocketState.Open)
                return;
            await socket.CloseOutputAsync(status, description, ct);
        }
        catch (WebSocketException)
        {
            // Peer already gone.
        }
        finally
        {
            _sendLock.Release();
        }
    }
}

/// <summary>Who a session acts as. Immutable, so a session's key and auth flag always change together.</summary>
internal sealed record SessionIdentity(string PairingKey, bool Authenticated);
