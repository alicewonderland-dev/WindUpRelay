using System.Net;
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

    private async Task StartAsync(int maxConnections, int maxPerIp, int? kestrelLimit = null)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { ContentRootPath = _dir, EnvironmentName = "Development" });
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        RelayWebSocketEndpoint.LimitUpgradedConnections(builder.WebHost, kestrelLimit ?? maxConnections);
        var app = builder.Build();
        app.UseWebSockets();
        var hub = new SessionHub(NullLogger.Instance, _store, 1000, RelayAuthMode.Bind);
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
        // The relay doesn't answer a client's close frame (it disposes the socket), so don't wait for one.
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
}
