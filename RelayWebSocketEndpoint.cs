using System.Net.WebSockets;
using WindUpKey.Protocol;

/// <summary>
/// The <c>/ws</c> endpoint: token check, connection admission (03-D9), then the session. Kept out of the top-level
/// program so tests can host it on a real Kestrel server.
/// </summary>
internal sealed class RelayWebSocketEndpoint(string relayToken, SessionHub sessions, ConnectionGate gate, ILogger log)
{
    private static readonly TimeSpan RefusalLogWindow = TimeSpan.FromMinutes(5);

    private readonly PerKeyThrottle _refusalLogs = new(RefusalLogWindow);
    private readonly PerKeyThrottle _headerLogs = new(RefusalLogWindow);

    /// <summary>
    /// The global cap (03-D9) is also Kestrel's own limit on upgraded connections, so nothing else can exceed it. The
    /// gate refuses first, with a 503; Kestrel refusing an upgrade the gate let through is answered with a 503 too.
    /// </summary>
    public static void LimitUpgradedConnections(ConfigureWebHostBuilder webHost, int maxConnections) =>
        webHost.ConfigureKestrel(options => options.Limits.MaxConcurrentUpgradedConnections = maxConnections);

    /// <summary>
    /// 05-D3: the keep-alive settings, from <c>Relay:KeepAliveIntervalSeconds</c> and <c>Relay:KeepAliveTimeoutSeconds</c>
    /// (defaults 30 and 90, the client's own). 0 turns that part off, for rollback: an interval of 0 sends no pings (so
    /// nothing can time out), a timeout of 0 keeps the pings but never drops a peer for a missing pong. Anything that
    /// isn't a whole, non-negative number of seconds gets the default (05-D11: only 0 means off).
    /// </summary>
    public static KeepAliveSettings KeepAliveFromConfig(IConfiguration configuration) => new(
        NonNegative(configuration["Relay:KeepAliveIntervalSeconds"], (int)WireLimits.KeepAliveInterval.TotalSeconds, 24 * 60 * 60),
        NonNegative(configuration["Relay:KeepAliveTimeoutSeconds"], (int)WireLimits.KeepAliveTimeout.TotalSeconds, 24 * 60 * 60));

    /// <summary>
    /// 05-D2: the first-bind limiter, from <c>Relay:FirstBindsPerIpPerHour</c> and <c>Relay:FirstBindsPerIpPerDay</c>
    /// (defaults 5 and 20 when a value is missing; the shipped appsettings.json turns both off until the live Funnel
    /// check passes, 05-D8). 0 turns a limit off; anything that isn't a whole, non-negative number gets the default
    /// (05-D11).
    /// </summary>
    public static FirstBindLimiter FirstBindsFromConfig(IConfiguration configuration, TimeProvider? timeProvider = null) => new(
        NonNegative(configuration["Relay:FirstBindsPerIpPerHour"], FirstBindLimiter.DefaultPerHour, 100_000),
        NonNegative(configuration["Relay:FirstBindsPerIpPerDay"], FirstBindLimiter.DefaultPerDay, 100_000),
        timeProvider);

    private static int NonNegative(string? raw, int fallback, int max) =>
        int.TryParse(raw, out var value) && value >= 0 ? Math.Min(value, max) : fallback;

    /// <summary>
    /// 05-D3: the one <see cref="WebSocketOptions"/> builder, shared by the relay and its tests. The relay pings every
    /// interval and aborts a connection whose pong hasn't come within the timeout (about interval + timeout after it went
    /// quiet), so a half-open peer no longer holds its session, its per-IP slot and its key's routing.
    /// </summary>
    public static WebSocketOptions BuildWebSocketOptions(KeepAliveSettings keepAlive)
    {
        var options = new WebSocketOptions
        {
            KeepAliveInterval = keepAlive.IntervalSeconds > 0 ? TimeSpan.FromSeconds(keepAlive.IntervalSeconds) : TimeSpan.Zero,
        };
        if (keepAlive.IntervalSeconds > 0 && keepAlive.TimeoutSeconds > 0)
            options.KeepAliveTimeout = TimeSpan.FromSeconds(keepAlive.TimeoutSeconds);
        return options;
    }

    public static bool IsAuthorized(HttpContext context, string relayToken)
    {
        var provided = RelayTokenAuth.GetProvidedToken(
            context.Request.Query["token"].FirstOrDefault(),
            context.Request.Headers.Authorization.FirstOrDefault());
        return RelayTokenAuth.IsAuthorized(provided, relayToken);
    }

    public async Task HandleAsync(HttpContext context)
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

        var forwardedFor = context.Request.Headers[ConnectionGate.ForwardedForHeader].LastOrDefault();
        var clientKey = ConnectionGate.ClientKey(context.Connection.RemoteIpAddress, forwardedFor);

        // For the live Funnel check (plans/IN-GAME-CHECKS.md): whether the header arrives, never its value.
        if (_headerLogs.TryAcquire("headers"))
        {
            log.LogInformation(
                "Client address headers: {ForwardedFor}={HasForwardedFor}, per-IP cap applies={Capped} (at most one log per 5 minutes)",
                ConnectionGate.ForwardedForHeader, !string.IsNullOrWhiteSpace(forwardedFor),
                clientKey is not null);
        }

        var admission = gate.TryEnter(clientKey, out var slot);
        if (admission != ConnectionAdmission.Admitted)
        {
            Refuse(context, admission.ToString());
            return;
        }

        // The slot is released when the session ends, however it ends.
        using (slot)
        {
            WebSocket socket;
            try
            {
                socket = await context.WebSockets.AcceptWebSocketAsync();
            }
            catch (InvalidOperationException) when (!context.Response.HasStarted)
            {
                // Kestrel's MaxConcurrentUpgradedConnections: a closing connection it still counts.
                Refuse(context, "KestrelUpgradeLimit");
                return;
            }

            // 05-D2: first binds are limited per client address too, with IPv6 per /48 (05-D7). Never logged.
            var firstBindKey = ConnectionGate.ClientKey(context.Connection.RemoteIpAddress, forwardedFor, FirstBindLimiter.Ipv6PrefixBits);
            using (socket)
                await sessions.HandleClientAsync(socket, firstBindKey, context.RequestAborted);
        }
    }

    private void Refuse(HttpContext context, string reason)
    {
        context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
        // A flood of refusals is logged at most once per reason per window, and never with the client's address.
        if (_refusalLogs.TryAcquire(reason))
            log.LogWarning("Refused a relay connection: {Reason} (at most one log per 5 minutes)", reason);
    }
}

/// <summary>05-D3: keep-alive interval and timeout in seconds; 0 turns that part off.</summary>
internal readonly record struct KeepAliveSettings(int IntervalSeconds, int TimeoutSeconds);
