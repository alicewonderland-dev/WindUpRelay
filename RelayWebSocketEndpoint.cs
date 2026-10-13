using System.Net.WebSockets;

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

            using (socket)
                await sessions.HandleClientAsync(socket, context.RequestAborted);
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
