using System.Collections.Concurrent;
using System.Globalization;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
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
// 03-D9: a global cap on open WebSocket connections (also Kestrel's upgraded-connection limit), and a cap per client
// address taken from the Funnel header (see ConnectionGate).
var maxConnections = int.TryParse(builder.Configuration["Relay:MaxConnections"], out var mc)
    ? Math.Clamp(mc, 1, 100_000)
    : 1000;
var maxConnectionsPerIp = int.TryParse(builder.Configuration["Relay:MaxConnectionsPerIp"], out var mpi)
    ? Math.Clamp(mpi, 1, 100_000)
    : 16;

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
RelayWebSocketEndpoint.LimitUpgradedConnections(builder.WebHost, maxConnections);

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

// A consent or wind stamp far in the future would win last-write-wins until then. Pushes like that are rejected
// now; this deletes any stored before, and the doll's client re-pushes its real state when it finds no relay row.
// Rows within the allowed skew are untouched, so it runs on every start. Only the count is logged.
var deletedStamps = store.DeleteFutureSyncStamps(DateTimeOffset.UtcNow);
log.LogInformation("Startup cleanup: {Count} future-stamped consent/wind row(s) deleted", deletedStamps);

var sessions = new SessionHub(log, store, maxMessagesPerMinute, authMode);
var endpoint = new RelayWebSocketEndpoint(relayToken, sessions, new ConnectionGate(maxConnections, maxConnectionsPerIp), log);

app.UseWebSockets();
app.MapGet("/", () => Results.Text("WindUpKey relay. Connect plugins to /ws"));
app.MapGet("/stats", (HttpContext context) =>
{
    if (!RelayWebSocketEndpoint.IsAuthorized(context, relayToken))
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
app.Map("/ws", (RequestDelegate)endpoint.HandleAsync);

log.LogInformation(
    "WindUpKey relay listening on {Urls}; database={Db}; authMode={AuthMode}; maxConnections={Max}; maxConnectionsPerIp={PerIp}",
    urls, dbPath, authMode, maxConnections, maxConnectionsPerIp);
app.Run();

internal sealed class SessionHub(ILogger log, RelayStore store, int maxMessagesPerMinute, RelayAuthMode authMode)
{
    private static readonly TimeSpan WindOutboxTtl = TimeSpan.FromDays(7);
    private static readonly TimeSpan WindRequestCooldown = TimeSpan.FromHours(1);
    private static readonly TimeSpan OwnerOutboxTtl = TimeSpan.FromDays(14);
    private static readonly TimeSpan PairOutboxTtl = TimeSpan.FromDays(30);
    private static readonly TimeSpan RefusalRecordWindow = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan ReplacedCloseTimeout = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan CloseDrainTimeout = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan RegisterInFlightPoll = TimeSpan.FromMilliseconds(100);
    internal static readonly TimeSpan DefaultSendTimeout = TimeSpan.FromSeconds(5);
    private const int FlushBatchRows = 50;
    private const int FlushBatches = 20;

    /// <summary>Close description when a connection registers a second, different key (relay-only, never an error envelope).</summary>
    internal const string OneKeyPerConnection = "one_key_per_connection";

    /// <summary>What events record for a client's message type that isn't a known one (03-D7).</summary>
    internal const string UnknownMsgType = "unknown";

    private readonly ConcurrentDictionary<string, ClientSession> _byPairingKey =
        new(StringComparer.Ordinal);

    /// <summary>
    /// How long any send to a session may take: from another session (a recipient that stops reading counts as
    /// offline after it, instead of stalling the sender's loop) or on its own loop (pong, pairSync, flush, replies).
    /// A session that doesn't take a frame in time is aborted. Settable for tests.
    /// </summary>
    internal TimeSpan SendTimeout { get; init; } = DefaultSendTimeout;

    /// <summary>
    /// How long a connection has to register (03-D3). A registered or displaced session is never closed by it.
    /// Settable for tests.
    /// </summary>
    internal TimeSpan RegisterTimeout { get; init; } = WireLimits.RegisterTimeout;

    private readonly PerKeyThrottle _refusalRecords = new(RefusalRecordWindow);
    private readonly PerKeyThrottle _keySwitchRecords = new(RefusalRecordWindow);
    private readonly PerKeyThrottle _authFailureRecords = new(RefusalRecordWindow);
    private readonly PerKeyThrottle _notPairedRecords = new(RefusalRecordWindow);
    private readonly PerKeyThrottle _futureStampRecords = new(RefusalRecordWindow);
    private readonly PerKeyThrottle _malformedRecords = new(RefusalRecordWindow);
    private readonly PerKeyThrottle _rateLimitedRecords = new(RefusalRecordWindow);
    private readonly PerKeyThrottle _tooBigRecords = new(RefusalRecordWindow);
    private readonly PerKeyThrottle _binaryRecords = new(RefusalRecordWindow);
    private readonly PerKeyThrottle _registerTimeoutRecords = new(RefusalRecordWindow);
    private readonly PerKeyThrottle _registerRequiredRecords = new(RefusalRecordWindow);
    private readonly PerKeyThrottle _routedTooBigRecords = new(RefusalRecordWindow);
    private readonly PerKeyThrottle _queueFullRecords = new(RefusalRecordWindow);
    private readonly PerKeyThrottle _outboxRowRecords = new(RefusalRecordWindow);

    public int OnlineCount => _byPairingKey.Count(kv => kv.Value.IsOpen);

    public async Task HandleClientAsync(WebSocket socket, CancellationToken ct)
    {
        var session = new ClientSession(socket, SendTimeout);
        using var deadlineCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var deadline = EnforceRegisterDeadlineAsync(session, socket, deadlineCts.Token);
        try
        {
            // 03-D16: an oversized or binary message is closed on as soon as it's known, not read to its end.
            var reader = new BoundedMessageReader(stopAtLimit: true);
            var beforeRegister = 0;
            while (socket.State == WebSocketState.Open && !ct.IsCancellationRequested)
            {
                // 03-D2: by RegisteredKey, not PairingKey: a displaced session has no PairingKey but did register.
                var registered = session.RegisteredKey is not null;
                var message = await reader.ReadAsync(
                    socket, registered ? WireLimits.RelayMessageBytes : WireLimits.PreRegisterMessageBytes, ct);
                if (message.Kind == WireMessageKind.Close)
                    break;

                // Once this session has been closed (refused, replaced or past its deadline), ignore what the client still sends.
                if (socket.State != WebSocketState.Open)
                    break;

                if (message.Kind == WireMessageKind.Oversized)
                {
                    RecordLimit(_tooBigRecords, "message_too_big", session);
                    await session.CloseAsync(WebSocketCloseStatus.MessageTooBig, WireCloseReasons.MessageTooBig, ct);
                    break;
                }

                if (message.Kind == WireMessageKind.Binary)
                {
                    RecordLimit(_binaryRecords, "binary_message", session);
                    await session.CloseAsync(WebSocketCloseStatus.InvalidMessageType, WireCloseReasons.BinaryNotSupported, ct);
                    break;
                }

                // 03-D3: a few messages to register in, pings included; old clients send their register first.
                if (!registered && ++beforeRegister > WireLimits.PreRegisterMessages)
                {
                    RecordLimit(_registerRequiredRecords, "register_required", session);
                    await session.CloseAsync(WebSocketCloseStatus.PolicyViolation, WireCloseReasons.RegisterRequired, ct);
                    break;
                }

                await ProcessMessageAsync(session, message.Text!, ct);
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
            await deadlineCts.CancelAsync();
            await deadline;
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

    /// <summary>
    /// 03-D3: closes a connection that hasn't registered within <see cref="RegisterTimeout"/>, and aborts it if the peer
    /// doesn't answer the close within the drain window (its loop may be blocked in a receive). A session that has
    /// registered, or is registering, is left alone, displaced or not. Ends quietly when <paramref name="ct"/> is
    /// cancelled (the session ended).
    /// </summary>
    private async Task EnforceRegisterDeadlineAsync(ClientSession session, WebSocket socket, CancellationToken ct)
    {
        try
        {
            await Task.Delay(RegisterTimeout, ct);

            // 03-D18: a register in flight is waited for; one that fails (a legacy key collision) leaves the session to
            // the deadline again.
            while (!session.TryExpireRegister())
            {
                if (session.RegisteredKey is not null)
                    return;
                await Task.Delay(RegisterInFlightPoll, ct);
            }

            RecordLimit(_registerTimeoutRecords, "register_timeout", session);
            await session.CloseAsync(WebSocketCloseStatus.PolicyViolation, WireCloseReasons.RegisterTimeout, ct);
            await Task.Delay(CloseDrainTimeout, ct);
            if (socket.State != WebSocketState.Closed)
                session.Abort();
        }
        catch (OperationCanceledException)
        {
            // The session ended first.
        }
        catch (Exception ex)
        {
            log.LogDebug(ex, "Register deadline failed");
        }
    }

    /// <summary>Records a limit event (03-D7: every 03 event is throttled), keyed by the registered key or, before register, one shared slot.</summary>
    private void RecordLimit(PerKeyThrottle throttle, string kind, ClientSession session, string? msgType = null)
    {
        var key = session.RegisteredKey;
        if (!throttle.TryAcquire(key ?? string.Empty))
            return;
        store.RecordEvent(kind, pairingKey: key, msgType: msgType);
        log.LogInformation("Relay limit: {Kind} key={Key}", kind, key ?? "(unregistered)");
    }

    /// <summary>A client's message type as events record it: a known <see cref="MessageTypes"/> constant, or "unknown" (03-D7).</summary>
    private static string EventMsgType(string? type) => MessageTypes.IsKnown(type) ? type! : UnknownMsgType;

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

        // 03-D8: a re-register counts too (one key per connection still holds); only pings and pongs are exempt.
        if (session.PairingKey is { } rateKey
            && envelope.Type is not (MessageTypes.Ping or MessageTypes.Pong)
            && !store.TryConsumeRate(rateKey, maxMessagesPerMinute))
        {
            // 03-D7: recorded at most once per key per window, and only with a known message type.
            if (_rateLimitedRecords.TryAcquire(rateKey))
                store.RecordEvent("rate_limited", pairingKey: rateKey, msgType: EventMsgType(envelope.Type));
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                Code = ErrorCodes.RateLimited,
                Message = "Too many messages; try again shortly.",
            }), ct);
            return;
        }

        try
        {
            await DispatchAsync(session, envelope, ct);
        }
        catch (JsonException) when (envelope.Type != MessageTypes.Register)
        {
            // A payload that doesn't deserialise (bad JSON types) is this message's problem, not the session's.
            RecordMalformed(session, envelope.Type);
        }
    }

    /// <summary>
    /// A client sending broken (or, 03-D4, over-limit) payloads may do so on every message: record and log at most once
    /// per key per window.
    /// </summary>
    private void RecordMalformed(ClientSession session, string msgType)
    {
        var key = session.PairingKey;
        if (!_malformedRecords.TryAcquire(key ?? string.Empty))
            return;
        store.RecordEvent("payload_malformed", pairingKey: key, msgType: EventMsgType(msgType));
        log.LogInformation("Dropped a {Type} with an unreadable or over-limit payload key={Key}", EventMsgType(msgType), key);
    }

    private async Task DispatchAsync(ClientSession session, Envelope envelope, CancellationToken ct)
    {
        // 03-D4: a field over its limit makes the payload malformed. It's dropped with no reply, before anything could
        // echo it (bad_request, not_allowed) or forward, queue or store it. 01-D7 still comes first: a routed relay-only
        // error is dropped silently by RouteReplyAsync, whatever else it holds. Past this point a payload's key fields
        // are keys, so handlers don't check them for blanks again (03-D18).
        if (!IsRelayOnlyError(envelope) && !WireLimits.IsValidPayload(envelope.Type, envelope.Payload))
        {
            RecordMalformed(session, envelope.Type);
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
                await RouteReplyAsync(session, envelope, ct);
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

    private static bool IsRelayOnlyError(Envelope envelope) =>
        envelope.Type == MessageTypes.Error && ErrorCodes.IsRelayOnly(envelope.GetPayload<ErrorPayload>()?.Code);

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

        // 03-D18: claim the connection from the register deadline before the auth decision (which may create an
        // account), so a session the deadline closed never claims or displaces a key.
        if (!session.TryBeginRegister())
            return;
        try
        {
            await RegisterAsync(session, pairingKey, payload?.Secret, ct);
        }
        finally
        {
            session.EndFailedRegister();
        }
    }

    private async Task RegisterAsync(ClientSession session, string pairingKey, string? secret, CancellationToken ct)
    {
        var outcome = DecideRegisterAuth(pairingKey, secret);
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

        // Only established pairs, and consent only toward them: a consent row alone never makes a partner.
        var peerKeys = store.GetPeerKeys(pairingKey).ToList();
        var paired = peerKeys.ToHashSet(StringComparer.Ordinal);
        var consent = store.GetPairConsentForDoll(pairingKey).Where(c => paired.Contains(c.PeerKey)).ToList();
        var wind = store.GetDollWind(pairingKey);

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

        // Legacy (unauthenticated) sessions still push during rollout, under the same rules.
        var now = DateTimeOffset.UtcNow;
        if (!string.IsNullOrEmpty(payload.WindUpdatedUtc))
        {
            string? expiryUtc = payload.ClearWind ? null : payload.ExpiryUtc;
            if (IsTooFarAhead(payload.WindUpdatedUtc, now))
            {
                RecordFutureStamp(dollKey, "wind");
            }
            else if (store.TryMergeDollWind(dollKey, expiryUtc, payload.WindUpdatedUtc))
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

        var unpaired = 0;
        foreach (var peer in payload.Peers)
        {
            var peerKey = NormalizePairingKey(peer.PeerKey);
            if (peerKey.Length != 8 || string.Equals(peerKey, dollKey, StringComparison.Ordinal))
                continue;

            // Consent is only kept for partners; anything else would let a push restore or invent a pair's state.
            if (!store.HasEstablishedPair(dollKey, peerKey))
            {
                unpaired++;
                continue;
            }

            if (IsTooFarAhead(peer.UpdatedUtc, now))
            {
                RecordFutureStamp(dollKey, "consent");
                continue;
            }

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

        if (unpaired > 0)
        {
            store.RecordEvent(
                "pair_consent_unpaired",
                pairingKey: dollKey,
                msgType: MessageTypes.DollStatePush,
                detail: unpaired.ToString(CultureInfo.InvariantCulture));
        }
    }

    private static bool IsTooFarAhead(string? stamp, DateTimeOffset now) =>
        SyncStamp.TryParse(stamp, out var parsed) && SyncStamp.IsTooFarAhead(parsed, now);

    /// <summary>A client with a fast clock pushes on every change: record and log at most once per key per window.</summary>
    private void RecordFutureStamp(string dollKey, string what)
    {
        if (!_futureStampRecords.TryAcquire(dollKey))
            return;
        store.RecordEvent("future_stamp_rejected", pairingKey: dollKey, msgType: MessageTypes.DollStatePush, detail: what);
        log.LogInformation("Rejected a {What} stamp too far in the future key={Key}", what, dollKey);
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

        if (await TryDeliverAsync(peerKey, toKey, MessageTypes.PairEstablished, envelope))
            return;

        // 03-D6: exempt from the quotas, like the removal notices.
        Enqueue(null, peerKey, toKey, MessageTypes.PairEstablished, envelope, PairOutboxTtl);
    }

    private async Task HandlePairRemoveAsync(ClientSession session, Envelope envelope, CancellationToken ct)
    {
        var payload = envelope.GetPayload<PairRemovePayload>();
        if (payload is null || session.PairingKey is null)
            return;

        var peerKey = NormalizePairingKey(payload.PeerKey);
        if (peerKey.Length != 8)
            return;

        var removal = store.RemovePair(session.PairingKey, peerKey);
        store.RecordEvent(
            "pair_remove",
            pairingKey: session.PairingKey,
            peerKey: peerKey,
            detail: removal.Purged > 0 ? $"purged={removal.Purged}" : null);

        var notify = Envelope.Create(MessageTypes.PairRemove, new PairRemovePayload
        {
            PeerKey = session.PairingKey,
        });

        if (await TryDeliverAsync(session.PairingKey, peerKey, MessageTypes.PairRemove, notify))
            return;

        // Delivered live unconditionally, but queued for an offline peer only when it removed an established pair or the
        // peer's own pending submit: otherwise there's nothing for the peer to drop. One pending notice per sender and
        // peer is enough, so nobody can fill a key's outbox with them.
        if (!removal.Removed)
            return;

        Enqueue(null, session.PairingKey, peerKey, MessageTypes.PairRemove, notify, PairOutboxTtl, once: true);
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
        if (payload is null)
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

        // Presence is only for partners. An unpaired target gets exactly the offline answer and no forward, so the
        // sender can't tell "unpaired" from "offline" or "unknown". A target whose socket fails is offline too.
        var online = false;
        if (MayRoute(session.PairingKey, payload.To))
        {
            var forward = Envelope.Create(MessageTypes.PresenceQuery, payload);
            online = FitsRouted(session.PairingKey, payload.To, MessageTypes.PresenceQuery, forward)
                     && await TryDeliverAsync(session.PairingKey, payload.To, MessageTypes.PresenceQuery, forward);
        }
        else
        {
            RecordNotPaired(session.PairingKey, payload.To, MessageTypes.PresenceQuery);
        }

        await session.SendAsync(Envelope.Create(MessageTypes.PresenceResult, new PresenceResultPayload
        {
            RequestId = payload.RequestId,
            From = payload.To,
            To = session.PairingKey,
            Online = online,
        }), ct);
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
        if (payload is null || payload.Hours <= 0)
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
        if (!await RequirePairAsync(session, payload.RequestId, payload.To, MessageTypes.Wind, ct))
            return;

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
        if (payload is null)
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
        if (!await RequirePairAsync(session, payload.RequestId, payload.To, MessageTypes.Unwind, ct))
            return;

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

        if (!store.TryConsumeWindRequest(session.PairingKey, WindRequestCooldown, out var cooldown))
        {
            await session.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                RequestId = payload.RequestId,
                Code = ErrorCodes.WindRequestCooldown,
                Message = "wind requests are limited to one per hour",
            }), ct);
            return;
        }

        // 03-D18: a request the recipient's full queue refused (queue_full) doesn't use up the hourly cooldown.
        var forward = Envelope.Create(MessageTypes.WindRequest, payload);
        await DeliverOrQueueAsync(
            session,
            payload.RequestId,
            payload.To,
            MessageTypes.WindRequest,
            forward,
            WindOutboxTtl,
            ct,
            onQueueFull: () => store.RefundWindRequest(cooldown));
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
        if (payload is null)
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
        if (!await RequirePairAsync(session, null, payload.To, MessageTypes.OwnerGrant, ct))
            return;

        var identity = payload.Identity;
        var live = Envelope.Create(MessageTypes.OwnerGrant, payload);
        if (!FitsRouted(session.PairingKey, payload.To, MessageTypes.OwnerGrant, live))
            return;
        if (await TryDeliverAsync(session.PairingKey, payload.To, MessageTypes.OwnerGrant, live))
            return;

        // 03-D6: a doll re-sends its grants on every connect, so at most one waits per doll and owner (a later revoke
        // still deletes it, 02-D31). It has no requestId, so a full queue only records an event.
        payload.Identity = null;
        var queued = Envelope.Create(MessageTypes.OwnerGrant, payload);
        Enqueue(null, session.PairingKey, payload.To, MessageTypes.OwnerGrant, queued, OwnerOutboxTtl, once: true);
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
        if (payload is null)
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

        // 02-D28: delivered or queued only between keys with an established pair (or to oneself). The client sends it
        // just before its pairRemove, so nothing is lost; a refused one is dropped silently, with a throttled event.
        if (!MayRoute(session.PairingKey, payload.To))
        {
            RecordNotPaired(session.PairingKey, payload.To, MessageTypes.OwnerRevoked);
            return;
        }

        var forward = Envelope.Create(MessageTypes.OwnerRevoked, payload);
        if (!FitsRouted(session.PairingKey, payload.To, MessageTypes.OwnerRevoked, forward))
            return;
        if (await TryDeliverAsync(session.PairingKey, payload.To, MessageTypes.OwnerRevoked, forward))
            return;

        // 02-D29: at most one pending copy per sender and recipient. RemovePair keeps it, so the pairRemove that
        // usually follows doesn't drop it. 02-D31: the grants queued before it go, so none is delivered after it.
        // 03-D6: exempt from the quotas, so a revoke is never lost to a full queue.
        Enqueue(null, session.PairingKey, payload.To, MessageTypes.OwnerRevoked, forward, OwnerOutboxTtl, once: true,
            supersedes: MessageTypes.OwnerGrant);
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
        if (payload is null)
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
        if (!await RequirePairAsync(session, payload.RequestId, payload.To, MessageTypes.OwnerSettingsQuery, ct))
            return;

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
        if (payload is null)
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
        if (!await RequirePairAsync(session, payload.RequestId, payload.To, MessageTypes.OwnerSettingsUpdate, ct))
            return;

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
        if (payload is null || payload.WorldId == 0 || payload.TerritoryId == 0)
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
        if (!await RequirePairAsync(session, payload.RequestId, payload.To, MessageTypes.Call, ct))
            return;

        var forward = Envelope.Create(MessageTypes.Call, payload);
        if (!FitsRouted(session.PairingKey, payload.To, MessageTypes.Call, forward))
            return;
        if (await TryDeliverAsync(session.PairingKey, payload.To, MessageTypes.Call, forward))
            return;

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
        CancellationToken ct,
        Action? onQueueFull = null)
    {
        if (sender.PairingKey is not { } fromKey)
            return;

        if (!FitsRouted(fromKey, toKey, msgType, forward))
            return;

        if (await TryDeliverAsync(fromKey, toKey, msgType, forward))
            return;

        var queued = Enqueue(requestId, fromKey, toKey, msgType, forward, ttl);
        if (queued == OutboxEnqueueStatus.QuotaFull)
            onQueueFull?.Invoke();
        if (string.IsNullOrEmpty(requestId))
            return;

        if (queued == OutboxEnqueueStatus.Queued)
        {
            await sender.SendAsync(Envelope.Create(MessageTypes.DeliveryQueued, new DeliveryStatusPayload
            {
                RequestId = requestId,
                To = toKey,
                Type = msgType,
            }), ct);
        }
        else if (queued == OutboxEnqueueStatus.QuotaFull)
        {
            // 03-D6: the newest message is the one refused, and the sender is told (queue_full isn't relay-only).
            await sender.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                RequestId = requestId,
                To = fromKey,
                Code = ErrorCodes.QueueFull,
                Message = "That partner's message queue is full; try again later.",
            }), ct);
        }
    }

    /// <summary>
    /// Queues <paramref name="envelope"/> for offline <paramref name="toKey"/> within the 03-D6 limits (row size, quotas,
    /// and with <paramref name="once"/> at most one pending of the type per sender and recipient), and records what
    /// happened. A full queue or an oversized row is recorded at most once per sender per window.
    /// </summary>
    private OutboxEnqueueStatus Enqueue(
        string? requestId,
        string fromKey,
        string toKey,
        string msgType,
        Envelope envelope,
        TimeSpan ttl,
        bool once = false,
        string? supersedes = null)
    {
        var result = store.TryEnqueueOutbox(requestId, fromKey, toKey, msgType, envelope.Serialize(), ttl, once, supersedes);
        switch (result)
        {
            case OutboxEnqueueStatus.Queued:
                store.RecordEvent("queue", pairingKey: fromKey, peerKey: toKey, msgType: msgType);
                break;
            case OutboxEnqueueStatus.QuotaFull when _queueFullRecords.TryAcquire(fromKey):
                store.RecordEvent("queue_full", pairingKey: fromKey, peerKey: toKey, msgType: msgType);
                log.LogInformation("Outbox full for key={To}; refused a {Type} from key={From}", toKey, msgType, fromKey);
                break;
            case OutboxEnqueueStatus.RowTooBig when _outboxRowRecords.TryAcquire(fromKey):
                store.RecordEvent("outbox_row_too_big", pairingKey: fromKey, peerKey: toKey, msgType: msgType);
                break;
        }

        return result;
    }

    /// <summary>
    /// 03-D5: whether an envelope built from a client's data is small enough to route, after re-serialisation (escaping
    /// can grow text up to six times). Otherwise it's dropped, neither delivered nor queued, and recorded at most once
    /// per sender per window. The largest routed message then takes about a second at 0.5 Mbit/s, well inside the send
    /// timeout, so a partner can't use one to get the recipient aborted.
    /// </summary>
    private bool FitsRouted(string fromKey, string toKey, string msgType, Envelope routed)
    {
        if (Encoding.UTF8.GetByteCount(routed.Serialize()) <= WireLimits.RoutedMessageBytes)
            return true;

        if (_routedTooBigRecords.TryAcquire(fromKey))
        {
            store.RecordEvent("routed_too_big", pairingKey: fromKey, peerKey: toKey, msgType: msgType);
            log.LogInformation("Dropped a {Type} from key={From} to key={To}: too big once re-serialised", msgType, fromKey, toKey);
        }

        return false;
    }

    /// <summary>
    /// Sends to the open session holding <paramref name="toKey"/>. False when it's offline or the send didn't happen
    /// (its socket died or was aborted meanwhile, or it didn't take the frame within <see cref="SendTimeout"/>;
    /// recorded as <c>send_failed</c>): callers then treat it as offline. A recipient's failure never throws into the
    /// sender's loop. The send is bounded by its own timeout only, never by the sender's token: the sender
    /// disconnecting must not abort the recipient's socket.
    /// </summary>
    private async Task<bool> TryDeliverAsync(string? fromKey, string toKey, string msgType, Envelope envelope)
    {
        if (!_byPairingKey.TryGetValue(toKey, out var target) || !target.IsOpen)
            return false;

        if (await target.TrySendAsync(envelope, SendTimeout))
            return true;

        store.RecordEvent("send_failed", pairingKey: fromKey, peerKey: toKey, msgType: msgType);
        log.LogDebug("Send to key={Key} failed; treated as offline", toKey);
        return false;
    }

    /// <summary>Messages to oneself (the debug self-pair) are always allowed; anything else needs an established pair.</summary>
    private bool MayRoute(string fromKey, string toKey) =>
        RoutingRules.MayRoute(fromKey, toKey, () => store.HasEstablishedPair(fromKey, toKey));

    /// <summary>
    /// True when <paramref name="sender"/> may address <paramref name="toKey"/>. Otherwise nothing is forwarded or
    /// queued, and the sender gets a generic <c>not_allowed</c> for <paramref name="requestId"/>. Without a requestId
    /// (ownerGrant) there's nothing for the client to match it to, and old and new clients alike would print a wind
    /// failure for it (a doll re-sends its grants on every connect), so it's dropped silently.
    /// </summary>
    private async Task<bool> RequirePairAsync(ClientSession sender, string? requestId, string toKey, string msgType, CancellationToken ct)
    {
        if (sender.PairingKey is not { } fromKey)
            return false;
        if (MayRoute(fromKey, toKey))
            return true;

        RecordNotPaired(fromKey, toKey, msgType);
        if (!string.IsNullOrEmpty(requestId))
        {
            await sender.SendAsync(Envelope.Create(MessageTypes.Error, new ErrorPayload
            {
                RequestId = requestId,
                To = fromKey,
                Code = ErrorCodes.NotAllowed,
                Message = "not allowed",
            }), ct);
        }

        return false;
    }

    /// <summary>Probes or a relay host missing the pair: record at most once per sender per window.</summary>
    private void RecordNotPaired(string fromKey, string toKey, string msgType)
    {
        if (_notPairedRecords.TryAcquire(fromKey))
            store.RecordEvent("route_not_paired", pairingKey: fromKey, peerKey: toKey, msgType: msgType);
    }

    private async Task FlushOutboxAsync(ClientSession session, CancellationToken ct)
    {
        if (session.PairingKey is not { } key)
            return;

        store.ExpirePending();

        // 03-D6: batches in id order until the outbox is empty or a send fails, at most FlushBatches per register.
        for (var batch = 0; batch < FlushBatches; batch++)
        {
            var rows = store.TakePendingFor(key, FlushBatchRows);
            if (rows.Count == 0)
                return;
            if (!await FlushRowsAsync(session, key, rows, ct))
                return;
        }
    }

    /// <summary>Delivers one batch of queued rows in order. False when a send failed: the rest stays pending.</summary>
    private async Task<bool> FlushRowsAsync(ClientSession session, string key, IReadOnlyList<OutboxRow> rows, CancellationToken ct)
    {
        foreach (var row in rows)
        {
            var envelope = Envelope.TryParse(row.EnvelopeJson);
            if (envelope is null)
            {
                store.MarkDelivered(row.Id);
                continue;
            }

            // The pair may be gone since this was queued (e.g. on another relay host, or a removal racing it).
            if (RoutingRules.RequiresPair(row.MsgType) && !MayRoute(row.FromKey, key))
            {
                store.MarkExpired(row.Id);
                store.RecordEvent("outbox_drop_unpaired", pairingKey: row.FromKey, peerKey: key, msgType: row.MsgType);
                continue;
            }

            // 03-D5: a row queued before these limits (by an older relay) may be too big to route now.
            if (!FitsRouted(row.FromKey, key, row.MsgType, envelope))
            {
                store.MarkExpired(row.Id);
                continue;
            }

            // Delivered only when the frame was actually sent; the rest stays pending for the next register.
            if (!await session.SendAsync(envelope, ct))
            {
                store.RecordEvent("send_failed", pairingKey: row.FromKey, peerKey: key, msgType: row.MsgType);
                return false;
            }

            store.MarkDelivered(row.Id);
            store.RecordEvent("deliver", pairingKey: row.FromKey, peerKey: key, msgType: row.MsgType);

            if (string.IsNullOrEmpty(row.RequestId))
                continue;

            await TryDeliverAsync(key, row.FromKey, MessageTypes.DeliveryDelivered, Envelope.Create(
                MessageTypes.DeliveryDelivered,
                new DeliveryStatusPayload
                {
                    RequestId = row.RequestId,
                    To = key,
                    Type = row.MsgType,
                }));
        }

        return true;
    }

    /// <summary>
    /// Routes a reply to its <c>to</c>. The relay sets <c>from</c> to the sender's own key, whatever the sender wrote,
    /// and delivers only between keys with an established pair (or to oneself). A reply that may not be delivered is
    /// dropped without an answer, since an error back would itself be a reply.
    /// </summary>
    private async Task RouteReplyAsync(ClientSession session, Envelope envelope, CancellationToken ct)
    {
        if (session.PairingKey is not { } fromKey)
            return;

        // Relay-only session codes can't be forged by addressing an error to another key.
        if (envelope.Type == MessageTypes.Error
            && ErrorCodes.IsRelayOnly(envelope.GetPayload<ErrorPayload>()?.Code))
            return;

        var reply = envelope.Type switch
        {
            MessageTypes.WindResult => Restamp<WindResultPayload>(
                envelope, fromKey, p => p.To, p => p.RequestId, (p, from, to) => (p.From, p.To) = (from, to)),
            MessageTypes.PresenceResult => Restamp<PresenceResultPayload>(
                envelope, fromKey, p => p.To, p => p.RequestId, (p, from, to) => (p.From, p.To) = (from, to)),
            MessageTypes.OwnerSettingsResult => Restamp<OwnerSettingsResultPayload>(
                envelope, fromKey, p => p.To, p => p.RequestId, (p, from, to) => (p.From, p.To) = (from, to)),
            MessageTypes.OwnerSettingsAck => Restamp<OwnerSettingsAckPayload>(
                envelope, fromKey, p => p.To, p => p.RequestId, (p, from, to) => (p.From, p.To) = (from, to)),
            MessageTypes.CallAck => Restamp<CallAckPayload>(
                envelope, fromKey, p => p.To, p => p.RequestId, (p, from, to) => (p.From, p.To) = (from, to)),
            MessageTypes.CallResult => Restamp<CallResultPayload>(
                envelope, fromKey, p => p.To, p => p.RequestId, (p, from, to) => (p.From, p.To) = (from, to)),
            MessageTypes.Error => Restamp<ErrorPayload>(
                envelope, fromKey, p => p.To, p => p.RequestId, (p, from, to) => (p.From, p.To) = (from, to)),
            _ => null,
        };

        if (reply is not { } readable || readable.To.Length != 8)
            return;

        var (routed, to, requestId) = readable;

        if (!MayRoute(fromKey, to))
        {
            RecordNotPaired(fromKey, to, envelope.Type);
            return;
        }

        if (!FitsRouted(fromKey, to, envelope.Type, routed))
            return;

        // Only the recipient of a queued message can ack it, and only toward the key that sent it.
        if (!string.IsNullOrEmpty(requestId)
            && envelope.Type is MessageTypes.WindResult or MessageTypes.OwnerSettingsResult or MessageTypes.OwnerSettingsAck
                or MessageTypes.CallAck or MessageTypes.CallResult)
        {
            store.MarkAckedByRequestId(requestId, ackerKey: fromKey, senderKey: to);
        }

        await TryDeliverAsync(fromKey, to, envelope.Type, routed);
    }

    /// <summary>The reply re-serialized with <c>from</c> set to the sender and <c>to</c> normalized, or null when unreadable.</summary>
    private static (Envelope Routed, string To, string? RequestId)? Restamp<T>(
        Envelope envelope,
        string fromKey,
        Func<T, string?> getTo,
        Func<T, string?> getRequestId,
        Action<T, string, string> stamp)
        where T : class
    {
        var payload = envelope.GetPayload<T>();
        if (payload is null)
            return null;

        var to = NormalizePairingKey(getTo(payload));
        stamp(payload, fromKey, to);
        return (Envelope.Create(envelope.Type, payload), to, getRequestId(payload));
    }

    /// <summary>The same normalisation the 03-D4 key checks use.</summary>
    internal static string NormalizePairingKey(string? key) => WireLimits.NormalizeKey(key);
}

/// <param name="sendTimeout">How long any one send may take (see <see cref="SessionHub.SendTimeout"/>).</param>
internal sealed class ClientSession(WebSocket socket, TimeSpan sendTimeout)
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

    // 03-D18: who has this connection before register, the register deadline or a register in flight; whichever
    // claims it first wins, so a deadline-closed session never claims or displaces a key.
    private const int Waiting = 0;
    private const int Registering = 1;
    private const int TimedOut = 2;
    private int _registerPhase;

    /// <summary>
    /// Called by a register before its auth decision. False once the register deadline has closed this connection: the
    /// register must stop. A connection that registered before stays <c>Registering</c>.
    /// </summary>
    public bool TryBeginRegister() =>
        Interlocked.CompareExchange(ref _registerPhase, Registering, Waiting) != TimedOut;

    /// <summary>Ends a register attempt that didn't register, so the deadline applies again.</summary>
    public void EndFailedRegister()
    {
        if (RegisteredKey is null)
            Interlocked.CompareExchange(ref _registerPhase, Waiting, Registering);
    }

    /// <summary>
    /// Called by the register deadline. True when no register has begun: the connection is the deadline's to close,
    /// and any later register stops. False while a register is in flight or after one succeeded.
    /// </summary>
    public bool TryExpireRegister() =>
        Interlocked.CompareExchange(ref _registerPhase, TimedOut, Waiting) == Waiting;

    public bool IsOpen => socket.State == WebSocketState.Open;

    /// <summary>Drops the connection without a close handshake (for a socket that is stuck).</summary>
    public void Abort() => socket.Abort();

    /// <summary>
    /// A send on this session's own loop (or from its takeover): bounded by <paramref name="ct"/> and by the send
    /// timeout. True when the frame was actually sent. False when it wasn't (see <see cref="TrySendCoreAsync"/>), or
    /// when this session didn't take it within the send timeout: it isn't reading, so it's aborted rather than left
    /// holding its send lock. Only <paramref name="ct"/> itself being cancelled throws. Cancelling
    /// <paramref name="ct"/> during the send aborts this socket, so another session's token must never be passed
    /// here: use <see cref="TrySendAsync"/>.
    /// </summary>
    public async Task<bool> SendAsync(Envelope envelope, CancellationToken ct)
    {
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bounded.CancelAfter(sendTimeout);
        try
        {
            return await TrySendCoreAsync(envelope, bounded.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // Timed out, sending or waiting for the send lock: this session stopped reading.
            Abort();
            return false;
        }
    }

    /// <summary>
    /// True when the frame was actually sent. False when the socket wasn't open, or it died or was aborted during the
    /// send (an abort surfaces as <see cref="OperationCanceledException"/> with <paramref name="ct"/> not cancelled):
    /// that is this session's peer failing, so it never throws into the loop of whichever session is sending to it.
    /// Only <paramref name="ct"/> being cancelled throws.
    /// </summary>
    private async Task<bool> TrySendCoreAsync(Envelope envelope, CancellationToken ct)
    {
        var bytes = Encoding.UTF8.GetBytes(envelope.Serialize());
        await _sendLock.WaitAsync(ct);
        try
        {
            if (socket.State != WebSocketState.Open)
                return false;
            await socket.SendAsync(bytes, WebSocketMessageType.Text, true, ct);
            return true;
        }
        catch (Exception ex) when (ex is WebSocketException or IOException or ObjectDisposedException
                                       || (ex is OperationCanceledException && !ct.IsCancellationRequested))
        {
            return false;
        }
        finally
        {
            _sendLock.Release();
        }
    }

    /// <summary>
    /// A send from another session: bounded by its own <paramref name="timeout"/>, not by the sending session's token.
    /// False when the frame wasn't sent, including when this session didn't take it in time. Either way that timeout
    /// aborts this socket, as a stuck one: whether the send itself was cut off, or it was still waiting for the send
    /// lock held by a stuck send. So partners don't stall on a wedged session again and again.
    /// </summary>
    public async Task<bool> TrySendAsync(Envelope envelope, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            return await TrySendCoreAsync(envelope, cts.Token);
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            // Timed out: this session stopped reading (or its send lock is held by such a send).
            Abort();
            return false;
        }
    }

    /// <summary>
    /// Sends a close frame without waiting for the peer's reply, so it is safe while this
    /// session's own loop is blocked in ReceiveAsync. Once the socket has left Open, that loop
    /// stops processing frames and drains until the peer's close (bounded) before disposing.
    /// </summary>
    public async Task CloseAsync(WebSocketCloseStatus status, string description, CancellationToken ct)
    {
        // Bounded like any send: a peer that doesn't take the close frame is aborted instead.
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bounded.CancelAfter(sendTimeout);
        try
        {
            await _sendLock.WaitAsync(bounded.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            Abort();
            return;
        }

        try
        {
            if (socket.State != WebSocketState.Open)
                return;
            await socket.CloseOutputAsync(status, description, bounded.Token);
        }
        catch (WebSocketException)
        {
            // Peer already gone.
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            Abort();
        }
        finally
        {
            _sendLock.Release();
        }
    }
}

/// <summary>Who a session acts as. Immutable, so a session's key and auth flag always change together.</summary>
internal sealed record SessionIdentity(string PairingKey, bool Authenticated);
