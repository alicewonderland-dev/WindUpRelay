using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using WindUpKey.Protocol;

namespace WindUpRelay.Tests;

/// <summary>
/// The <c>/ws</c> endpoint on a real Kestrel server: the 503 at the global and per-IP caps (03-D9), with the
/// forwarded address sent from loopback as tailscaled would, and the slots released when sessions end, however they end.
/// </summary>
public sealed class RelayWebSocketEndpointTests : IAsyncDisposable
{
    private const string Token = "test";
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "windup-relay-endpoint-tests-" + Guid.NewGuid().ToString("N"));
    private readonly RelayStore _store;
    private readonly List<ClientWebSocket> _sockets = [];
    private WebApplication? _app;
    private SessionHub? _hub;
    private Uri? _uri;

    public RelayWebSocketEndpointTests()
    {
        Directory.CreateDirectory(_dir);
        _store = new RelayStore(Path.Combine(_dir, "relay.db"));
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var socket in _sockets)
            socket.Dispose();
        if (_app is not null)
            await _app.DisposeAsync();
        _store.Dispose();
        SqliteConnection.ClearAllPools();
        try
        {
            Directory.Delete(_dir, recursive: true);
        }
        catch (IOException)
        {
            // Best effort temp cleanup.
        }
    }

    private async Task StartAsync(
        int maxConnections,
        int maxPerIp,
        int? kestrelLimit = null,
        FirstBindLimiter? firstBinds = null,
        KeepAliveSettings? keepAlive = null)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { ContentRootPath = _dir, EnvironmentName = "Development" });
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        RelayWebSocketEndpoint.LimitUpgradedConnections(builder.WebHost, kestrelLimit ?? maxConnections);
        var app = builder.Build();
        // The relay's own builder (05-D3); without settings, the framework's defaults as before 05.
        if (keepAlive is { } settings)
            app.UseWebSockets(RelayWebSocketEndpoint.BuildWebSocketOptions(settings));
        else
            app.UseWebSockets();
        var hub = new SessionHub(NullLogger.Instance, _store, 1000, RelayAuthMode.Bind)
        {
            FirstBinds = firstBinds ?? new FirstBindLimiter(FirstBindLimiter.DefaultPerHour, FirstBindLimiter.DefaultPerDay),
        };
        _hub = hub;
        var endpoint = new RelayWebSocketEndpoint(Token, hub, new ConnectionGate(maxConnections, maxPerIp), NullLogger.Instance);
        app.Map("/ws", (RequestDelegate)endpoint.HandleAsync);
        await app.StartAsync();
        _app = app;
        var address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        _uri = new Uri(address.Replace("http://", "ws://") + "/ws");
    }

    /// <summary>Connects, and returns the socket when upgraded or the HTTP status it was refused with.</summary>
    private async Task<(ClientWebSocket? Socket, HttpStatusCode Status)> ConnectAsync(string? forwardedFor = null, string? ingressSource = null)
    {
        var socket = new ClientWebSocket();
        socket.Options.SetRequestHeader("Authorization", "Bearer " + Token);
        if (forwardedFor is not null)
            socket.Options.SetRequestHeader("X-Forwarded-For", forwardedFor);
        if (ingressSource is not null)
            socket.Options.SetRequestHeader("Tailscale-Ingress-Src", ingressSource);
        socket.Options.CollectHttpResponseDetails = true;
        try
        {
            await socket.ConnectAsync(_uri!, CancellationToken.None);
            _sockets.Add(socket);
            return (socket, socket.HttpStatusCode);
        }
        catch (WebSocketException)
        {
            var status = socket.HttpStatusCode;
            socket.Dispose();
            return (null, status);
        }
    }

    /// <summary>Closes <paramref name="socket"/>, then waits until a connection like it is admitted again.</summary>
    private async Task<ClientWebSocket> CloseThenReconnectAsync(ClientWebSocket socket, string? forwardedFor)
    {
        // Only half-close: the slot must come back whether or not this client reads the relay's answer (05-D4).
        await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None);
        return await ReconnectWhenReleasedAsync(forwardedFor);
    }

    /// <summary>Waits until a connection with <paramref name="forwardedFor"/> is admitted again (a slot was released).</summary>
    private async Task<ClientWebSocket> ReconnectWhenReleasedAsync(string? forwardedFor)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (true)
        {
            var (again, status) = await ConnectAsync(forwardedFor);
            if (again is not null)
                return again;
            Assert.Equal(HttpStatusCode.ServiceUnavailable, status);
            Assert.True(DateTime.UtcNow < deadline, "the slot was not released");
            await Task.Delay(50);
        }
    }

    private static Task SendAsync(ClientWebSocket socket, string type, object payload) =>
        socket.SendAsync(Encoding.UTF8.GetBytes(Envelope.Create(type, payload).Serialize()), WebSocketMessageType.Text, true, CancellationToken.None);

    /// <summary>The next message's envelope, or null at the relay's close (whose description is then on the socket).</summary>
    private static async Task<Envelope?> ReceiveAsync(ClientWebSocket socket)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var buffer = new byte[16 * 1024];
        using var message = new MemoryStream();
        while (true)
        {
            var result = await socket.ReceiveAsync(buffer, timeout.Token);
            if (result.MessageType == WebSocketMessageType.Close)
                return null;
            message.Write(buffer, 0, result.Count);
            if (result.EndOfMessage)
                return Envelope.TryParse(Encoding.UTF8.GetString(message.ToArray()));
        }
    }

    private static async Task RegisterAsync(ClientWebSocket socket, string key, string? secret)
    {
        await SendAsync(socket, MessageTypes.Register, new RegisterPayload { PairingKey = key, Secret = secret });
        while ((await ReceiveAsync(socket))?.Type is { } type && type != MessageTypes.PairSync)
        {
        }

        Assert.Equal(WebSocketState.Open, socket.State);
    }

    [Fact]
    public async Task TheGlobalCap_RefusesTheUpgradeWith503_UntilASessionEnds()
    {
        await StartAsync(maxConnections: 2, maxPerIp: 16);
        var (first, _) = await ConnectAsync();
        var (second, _) = await ConnectAsync("203.0.113.7");
        Assert.NotNull(first);
        Assert.NotNull(second);

        var (refused, status) = await ConnectAsync("203.0.113.8");
        Assert.Null(refused);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, status);

        await CloseThenReconnectAsync(first!, null);
    }

    [Fact]
    public async Task ThePerIpCap_RefusesThatForwardedAddressOnly_AndNoHeaderMeansNoPerIpCap()
    {
        await StartAsync(maxConnections: 100, maxPerIp: 2);
        var (a1, _) = await ConnectAsync("203.0.113.7");
        var (a2, _) = await ConnectAsync("198.51.100.1, 203.0.113.7");
        Assert.NotNull(a1);
        Assert.NotNull(a2);

        var (refused, status) = await ConnectAsync("203.0.113.7");
        Assert.Null(refused);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, status);

        Assert.NotNull((await ConnectAsync("203.0.113.8")).Socket);
        for (var i = 0; i < 4; i++)
            Assert.NotNull((await ConnectAsync()).Socket);

        await CloseThenReconnectAsync(a1!, "203.0.113.7");
    }

    [Fact]
    public async Task KestrelsOwnUpgradeLimit_IsAnswered503Too()
    {
        await StartAsync(maxConnections: 100, maxPerIp: 16, kestrelLimit: 1);
        Assert.NotNull((await ConnectAsync()).Socket);

        var (refused, status) = await ConnectAsync();
        Assert.Null(refused);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, status);
    }

    [Fact]
    public async Task ASpoofedIngressHeader_IsNeverTrusted_SoItGetsNoPerIpCap()
    {
        // 03-D15: a client can send Tailscale-Ingress-Src itself (tailscaled doesn't strip it); only X-Forwarded-For counts.
        await StartAsync(maxConnections: 100, maxPerIp: 1);
        for (var i = 0; i < 3; i++)
            Assert.NotNull((await ConnectAsync(ingressSource: "203.0.113.7")).Socket);

        Assert.NotNull((await ConnectAsync("198.51.100.1", "203.0.113.7")).Socket);
        var (refused, status) = await ConnectAsync("198.51.100.1", "203.0.113.8");
        Assert.Null(refused);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, status);
    }

    [Fact]
    public async Task TheSlot_IsReleasedWhenTheSessionEndsWithAnException()
    {
        await StartAsync(maxConnections: 1, maxPerIp: 16);
        var (socket, _) = await ConnectAsync("203.0.113.7");
        await RegisterAsync(socket!, "KEYAAAA1", null);

        // With its events table gone, the session's end (recording "unregister") throws out of HandleClientAsync.
        using (var conn = new SqliteConnection($"Data Source={Path.Combine(_dir, "relay.db")}"))
        {
            conn.Open();
            using var drop = conn.CreateCommand();
            drop.CommandText = "DROP TABLE relay_events;";
            drop.ExecuteNonQuery();
        }

        await CloseThenReconnectAsync(socket!, "203.0.113.7");
    }

    [Fact]
    public async Task TheSlot_IsReleasedWhenASessionIsTakenOver()
    {
        await StartAsync(maxConnections: 2, maxPerIp: 16);
        var secret = RelaySecret.Generate();
        var (old, _) = await ConnectAsync("203.0.113.7");
        await RegisterAsync(old!, "KEYAAAA1", secret);
        var (replacement, _) = await ConnectAsync("203.0.113.7");
        await RegisterAsync(replacement!, "KEYAAAA1", secret);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await ConnectAsync("203.0.113.8")).Status);

        // The replaced client reads the relay's close and answers it, as plugins do; its slot then comes back.
        while (await ReceiveAsync(old!) is not null)
        {
        }

        Assert.Equal(ErrorCodes.SessionReplaced, old!.CloseStatusDescription);
        await old.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None);
        await ReconnectWhenReleasedAsync("203.0.113.8");
        Assert.Equal(WebSocketState.Open, replacement!.State);
    }

    // ---- 05-D2: first binds per client address ----

    private long CountEvents(string kind)
    {
        using var conn = new SqliteConnection($"Data Source={Path.Combine(_dir, "relay.db")}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM relay_events WHERE kind = $k;";
        cmd.Parameters.AddWithValue("$k", kind);
        return (long)cmd.ExecuteScalar()!;
    }

    /// <summary>Connects with <paramref name="forwardedFor"/>, registers, and closes; true when the register was accepted.</summary>
    private async Task<bool> BindAsync(string? forwardedFor, string key, string secret)
    {
        var (socket, _) = await ConnectAsync(forwardedFor);
        Assert.NotNull(socket);
        await SendAsync(socket!, MessageTypes.Register, new RegisterPayload { PairingKey = key, Secret = secret });
        while (await ReceiveAsync(socket!) is { } envelope)
        {
            if (envelope.Type != MessageTypes.PairSync)
                continue;
            await socket!.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None);
            return true;
        }

        Assert.Equal((WebSocketCloseStatus)1013, socket!.CloseStatus);
        Assert.Equal(WireCloseReasons.BindRateLimited, socket.CloseStatusDescription);
        await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None);
        return false;
    }

    private static string KeyNo(int i) => "BIND" + i.ToString("0000", System.Globalization.CultureInfo.InvariantCulture);

    [Fact]
    public async Task TheSixthFirstBindInAnHour_FromOneAddress_IsClosedWith1013_AndCreatesNoAccount()
    {
        await StartAsync(maxConnections: 100, maxPerIp: 16);
        var secrets = Enumerable.Range(0, 8).Select(_ => RelaySecret.Generate()).ToList();
        for (var i = 0; i < 5; i++)
            Assert.True(await BindAsync("203.0.113.7", KeyNo(i), secrets[i]));

        Assert.False(await BindAsync("203.0.113.7", KeyNo(5), secrets[5]));
        Assert.Null(_store.GetAccountSecretHash(KeyNo(5)));
        Assert.Equal(0, _hub!.OnlineCount);
        Assert.Equal(1, CountEvents("bind_rate_limited"));

        // Another address has its own limit; an existing account still authenticates from the limited one.
        Assert.True(await BindAsync("198.51.100.1", KeyNo(5), secrets[5]));
        Assert.True(await BindAsync("203.0.113.7", KeyNo(0), secrets[0]));
        Assert.Equal(6, _store.SnapshotStats().Accounts);
    }

    [Fact]
    public async Task FirstBindsFromIPv6_AreLimitedPerSlash48()
    {
        // 05-D7: a site's /64s share one limit (the per-IP cap still counts per /64).
        await StartAsync(maxConnections: 100, maxPerIp: 16);
        for (var i = 0; i < 5; i++)
            Assert.True(await BindAsync($"2001:db8:7:{i + 1:x}::1", KeyNo(i), RelaySecret.Generate()));

        Assert.False(await BindAsync("2001:db8:7:ffff::9", KeyNo(5), RelaySecret.Generate()));
        Assert.True(await BindAsync("2001:db8:8:1::1", KeyNo(6), RelaySecret.Generate()));
    }

    [Fact]
    public async Task WithoutAForwardedAddress_FirstBindsAreNotLimited()
    {
        await StartAsync(maxConnections: 100, maxPerIp: 16);
        for (var i = 0; i < 7; i++)
            Assert.True(await BindAsync(null, KeyNo(i), RelaySecret.Generate()));
        Assert.Equal(0, CountEvents("bind_rate_limited"));
    }

    [Fact]
    public async Task FirstBindLimitsOfZero_AreOff()
    {
        await StartAsync(maxConnections: 100, maxPerIp: 16, firstBinds: new FirstBindLimiter(0, 0));
        for (var i = 0; i < 7; i++)
            Assert.True(await BindAsync("203.0.113.7", KeyNo(i), RelaySecret.Generate()));
    }

    // ---- 05-D3: keep-alive (1 s / 2 s here) ----

    private static readonly KeepAliveSettings FastKeepAlive = new(1, 2);

    /// <summary>Registers <paramref name="socket"/> without reading its replies (it never reads again).</summary>
    private static Task RegisterWithoutReadingAsync(ClientWebSocket socket, string key, string secret) =>
        SendAsync(socket, MessageTypes.Register, new RegisterPayload { PairingKey = key, Secret = secret });

    [Fact]
    public async Task AClientThatNeverReads_IsDroppedByTheKeepAlive_FreeingItsSlotAndItsKey()
    {
        await StartAsync(maxConnections: 100, maxPerIp: 1, keepAlive: FastKeepAlive);
        var peer = await ConnectAsync();
        await RegisterAsync(peer.Socket!, "PEERBBB2", RelaySecret.Generate());
        _store.TryEstablishPair("KEYAAAA1", "PEERBBB2");

        // It never reads, so it never answers a ping (a client only does while a receive is pending).
        var (dead, _) = await ConnectAsync("203.0.113.7");
        await RegisterWithoutReadingAsync(dead!, "KEYAAAA1", RelaySecret.Generate());
        await Task.Delay(300);
        Assert.Equal(2, _hub!.OnlineCount);

        // The peer keeps a receive pending throughout, so it answers pings and stays.
        var peerNext = ReceiveAsync(peer.Socket!);

        // Dropped at about interval + timeout (3 s): its per-IP slot comes back, and it no longer counts as online.
        var started = DateTime.UtcNow;
        while (_hub.OnlineCount != 1)
        {
            Assert.True(DateTime.UtcNow - started < TimeSpan.FromSeconds(10), "the silent client was never dropped");
            await Task.Delay(100);
        }

        await ReconnectWhenReleasedAsync("203.0.113.7");
        Assert.Equal(1, CountEvents("keepalive_timeout"));

        // A wind to its key is queued now.
        await SendAsync(peer.Socket!, MessageTypes.Wind, new WindPayload { RequestId = "w-after", To = "KEYAAAA1", Hours = 1 });
        Assert.Equal(MessageTypes.DeliveryQueued, (await peerNext)!.Type);
    }

    [Fact]
    public async Task AClientThatKeepsReading_SurvivesTheKeepAlive()
    {
        await StartAsync(maxConnections: 100, maxPerIp: 16, keepAlive: FastKeepAlive);
        var (socket, _) = await ConnectAsync();
        await RegisterAsync(socket!, "KEYAAAA1", RelaySecret.Generate());

        // Three times interval + timeout, with a receive pending all along (so pings are answered).
        var pending = ReceiveAsync(socket!);
        await Task.Delay(TimeSpan.FromSeconds(9));
        Assert.False(pending.IsCompleted, "the relay closed or sent something");
        Assert.Equal(WebSocketState.Open, socket!.State);

        await SendAsync(socket, MessageTypes.Ping, new { });
        Assert.Equal(MessageTypes.Pong, (await pending)!.Type);
        Assert.Equal(1, _hub!.OnlineCount);
        Assert.Equal(0, CountEvents("keepalive_timeout"));
    }

    /// <summary>
    /// Upgrades a raw TCP connection by hand and registers <paramref name="key"/> on it, so the test can then reset the
    /// connection (an RST, as a crashed or unplugged client's OS sends), which <see cref="ClientWebSocket"/> can't.
    /// </summary>
    private async Task<TcpClient> RegisterOnRawConnectionAsync(string forwardedFor, string key)
    {
        var tcp = new TcpClient { NoDelay = true };
        await tcp.ConnectAsync(_uri!.Host, _uri.Port);
        var stream = tcp.GetStream();
        var request =
            $"GET /ws HTTP/1.1\r\nHost: {_uri.Authority}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
            + $"Sec-WebSocket-Key: {Convert.ToBase64String(Guid.NewGuid().ToByteArray())}\r\nSec-WebSocket-Version: 13\r\n"
            + $"Authorization: Bearer {Token}\r\nX-Forwarded-For: {forwardedFor}\r\n\r\n";
        await stream.WriteAsync(Encoding.ASCII.GetBytes(request));

        // The 101 response's head, byte by byte, so nothing after it is read here.
        var head = new StringBuilder();
        var one = new byte[1];
        while (!head.ToString().EndsWith("\r\n\r\n", StringComparison.Ordinal))
        {
            Assert.Equal(1, await stream.ReadAsync(one));
            head.Append((char)one[0]);
        }

        Assert.StartsWith("HTTP/1.1 101", head.ToString(), StringComparison.Ordinal);
        var socket = WebSocket.CreateFromStream(stream, new WebSocketCreationOptions { IsServer = false });
        var register = Envelope.Create(MessageTypes.Register, new RegisterPayload { PairingKey = key, Secret = RelaySecret.Generate() });
        await socket.SendAsync(Encoding.UTF8.GetBytes(register.Serialize()), WebSocketMessageType.Text, true, CancellationToken.None);
        return tcp;
    }

    [Fact]
    public async Task APeerThatDisposesOrResetsItsConnection_IsNoKeepAliveTimeout()
    {
        // 05-D13: only the keep-alive's own abort counts; a client that goes away (every character switch, Reconnect or
        // unload disposes without a close, and a crash resets) records no keepalive_timeout.
        await StartAsync(maxConnections: 100, maxPerIp: 1, keepAlive: FastKeepAlive);

        var (disposing, _) = await ConnectAsync("203.0.113.7");
        await RegisterAsync(disposing!, "KEYAAAA1", RelaySecret.Generate());
        disposing!.Dispose();
        // Its slot came back, so its session ended. The probe connection goes at once too (it would never answer a ping).
        (await ReconnectWhenReleasedAsync("203.0.113.7")).Dispose();

        var resetting = await RegisterOnRawConnectionAsync("198.51.100.1", "KEYBBBB2");
        var started = DateTime.UtcNow;
        while (_hub!.OnlineCount != 1)
        {
            Assert.True(DateTime.UtcNow - started < TimeSpan.FromSeconds(5), "the raw client never registered");
            await Task.Delay(50);
        }

        resetting.Client.LingerState = new LingerOption(true, 0);
        resetting.Dispose();
        (await ReconnectWhenReleasedAsync("198.51.100.1")).Dispose();

        // Well past interval + timeout (3 s), in case anything were still pending.
        await Task.Delay(TimeSpan.FromSeconds(4));
        Assert.Equal(0, CountEvents("keepalive_timeout"));
    }

    // ---- 05-D4: a takeover never leaks the old session ----

    [Fact]
    public async Task ATakeoverOfASessionThatStoppedReading_FreesItsSlot()
    {
        await StartAsync(maxConnections: 100, maxPerIp: 2);
        var secret = RelaySecret.Generate();
        var (silent, _) = await ConnectAsync("203.0.113.7");
        await RegisterWithoutReadingAsync(silent!, "KEYAAAA1", secret);
        await Task.Delay(300);
        var (replacement, _) = await ConnectAsync("203.0.113.7");
        await RegisterAsync(replacement!, "KEYAAAA1", secret);

        // The silent session never answers the relay's close; it's aborted after the drain window, freeing its slot
        // while the replacement keeps the other.
        await ReconnectWhenReleasedAsync("203.0.113.7");
        Assert.Equal(WebSocketState.Open, replacement!.State);
    }
}
