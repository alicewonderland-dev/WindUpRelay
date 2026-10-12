using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using System.Threading.Channels;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using WindUpKey.Protocol;

namespace WindUpRelay.Tests;

/// <summary>
/// Drives <see cref="SessionHub"/> over real loopback WebSockets (no web host), so register, takeover,
/// routing and close behaviour are tested end to end against the relay's own code.
/// </summary>
public sealed class SessionHubTests : IDisposable
{
    private const string Key = "KEYAAAA1";
    private const string Peer = "PEERBBB2";
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan Quiet = TimeSpan.FromMilliseconds(500);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "windup-relay-hub-tests-" + Guid.NewGuid().ToString("N"));
    private readonly string _dbPath;
    private readonly RelayStore _store;
    private readonly CancellationTokenSource _cts = new();
    private readonly List<TestClient> _clients = [];

    public SessionHubTests()
    {
        _dbPath = Path.Combine(_dir, "relay.db");
        _store = new RelayStore(_dbPath);
    }

    public void Dispose()
    {
        foreach (var client in _clients)
            client.Dispose();
        _cts.Cancel();
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

    private SessionHub NewHub(string mode) =>
        new(NullLogger.Instance, _store, 1000, Enum.Parse<RelayAuthMode>(mode));

    private async Task<TestClient> ConnectAsync(SessionHub hub, bool reading = true)
    {
        var client = await TestClient.ConnectAsync(hub, reading, _cts.Token);
        _clients.Add(client);
        return client;
    }

    private async Task<TestClient> RegisterOkAsync(SessionHub hub, string key, string? secret)
    {
        var client = await ConnectAsync(hub);
        await client.RegisterAsync(key, secret);
        await client.ExpectTypeAsync(MessageTypes.PairSync);
        return client;
    }

    private long CountEvents(string kind)
    {
        using var conn = new SqliteConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM relay_events WHERE kind = $k;";
        cmd.Parameters.AddWithValue("$k", kind);
        return (long)cmd.ExecuteScalar()!;
    }

    [Fact]
    public async Task AuthenticatedTakeover_MapsTheNewSession_ThenNotifiesAndClosesTheOld()
    {
        var hub = NewHub("Bind");
        var secret = RelaySecret.Generate();
        var old = await RegisterOkAsync(hub, Key, secret);
        var peer = await RegisterOkAsync(hub, Peer, null);

        var replacement = await RegisterOkAsync(hub, Key, secret);

        var (messages, status, description) = await old.UntilCloseAsync();
        var notice = Assert.Single(messages);
        Assert.Equal(MessageTypes.Error, notice.Type);
        var payload = notice.GetPayload<ErrorPayload>()!;
        Assert.Equal(ErrorCodes.SessionReplaced, payload.Code);
        Assert.Null(payload.To);
        Assert.Equal(WebSocketCloseStatus.PolicyViolation, status);
        Assert.Equal(ErrorCodes.SessionReplaced, description);

        // Traffic for the key reaches the new session.
        await peer.SendAsync(MessageTypes.PresenceQuery, new PresenceQueryPayload { RequestId = "pq", To = Key });
        var result = await peer.ExpectTypeAsync(MessageTypes.PresenceResult);
        Assert.True(result.GetPayload<PresenceResultPayload>()!.Online);
        await replacement.ExpectTypeAsync(MessageTypes.PresenceQuery);
        Assert.Equal(1, CountEvents("session_replaced"));
    }

    [Fact]
    public async Task AuthenticatedTakeover_OfALegacySession_ClosesWithoutAnErrorEnvelope()
    {
        var hub = NewHub("Bind");
        var legacy = await RegisterOkAsync(hub, Key, null);

        await RegisterOkAsync(hub, Key, RelaySecret.Generate());

        // Old clients print a chat error for unknown codes, so they only get the close frame.
        var (messages, status, description) = await legacy.UntilCloseAsync();
        Assert.Empty(messages);
        Assert.Equal(WebSocketCloseStatus.PolicyViolation, status);
        Assert.Equal(ErrorCodes.SessionReplaced, description);
    }

    [Fact]
    public async Task Takeover_DoesNotWaitOnAStuckOldSession()
    {
        var hub = NewHub("Bind");
        var secret = RelaySecret.Generate();
        var stuck = await ConnectAsync(hub, reading: false);
        await stuck.RegisterAsync(Key, secret);
        var peer = await RegisterOkAsync(hub, Peer, null);
        await Task.Delay(200);

        // Route enough traffic at the stuck session that the relay's send to it blocks (holding its send lock).
        var filler = new string('x', 32 * 1024);
        _ = Task.Run(async () =>
        {
            for (var i = 0; i < 64; i++)
                await peer.SendAsync(MessageTypes.Error, new ErrorPayload { To = Key, Code = ErrorCodes.NotAllowed, Message = filler });
        });
        await Task.Delay(500);

        var started = DateTime.UtcNow;
        var replacement = await ConnectAsync(hub);
        await replacement.RegisterAsync(Key, secret);
        await replacement.ExpectTypeAsync(MessageTypes.PairSync);
        Assert.True(DateTime.UtcNow - started < TimeSpan.FromSeconds(2), "the new session waited on the old one");
    }

    [Fact]
    public async Task StuckReplacedSession_IsStrippedAtTakeover_SoItsStatePushIsNotMerged()
    {
        var hub = NewHub("Bind");
        var secret = RelaySecret.Generate();
        var stuck = await ConnectAsync(hub, reading: false);
        await stuck.RegisterAsync(Key, secret);
        var peer = await RegisterOkAsync(hub, Peer, null);
        await Task.Delay(200);

        // Block the relay's sends to the old session, so its close is stuck and its socket stays open for a while.
        var filler = new string('x', 32 * 1024);
        _ = Task.Run(async () =>
        {
            for (var i = 0; i < 64; i++)
                await peer.SendAsync(MessageTypes.Error, new ErrorPayload { To = Key, Code = ErrorCodes.NotAllowed, Message = filler });
        });
        await Task.Delay(500);

        await RegisterOkAsync(hub, Key, secret);

        // The old socket is still open on the relay; whatever it pushes now must not count as the key's state.
        var now = DateTimeOffset.UtcNow;
        await stuck.SendAsync(MessageTypes.DollStatePush, new DollStatePushPayload
        {
            ExpiryUtc = now.AddHours(10).ToString("O"),
            WindUpdatedUtc = now.ToString("O"),
            Peers =
            [
                new PairConsentSyncEntry { PeerKey = Peer, CanWindMe = true, IsOwner = true, UpdatedUtc = now.ToString("O") },
            ],
        });
        await Task.Delay(Quiet);

        Assert.Null(_store.GetDollWind(Key));
        Assert.Equal(0, CountEvents("doll_wind_merge"));
        Assert.Equal(0, CountEvents("pair_consent_merge"));
    }

    [Fact]
    public async Task SecondKey_OnTheSameConnection_IsClosedSilently_AndRecordedOncePerWindow()
    {
        var hub = NewHub("Bind");
        var secret = RelaySecret.Generate();

        for (var attempt = 0; attempt < 2; attempt++)
        {
            var client = await RegisterOkAsync(hub, Key, secret);

            await client.RegisterAsync(Peer, RelaySecret.Generate());

            var (messages, status, description) = await client.UntilCloseAsync();
            Assert.Empty(messages);
            Assert.Equal(WebSocketCloseStatus.PolicyViolation, status);
            Assert.Equal(SessionHub.OneKeyPerConnection, description);
            await client.CloseOutputAsync();
        }

        // Refused before the auth decision: the second key wasn't claimed.
        Assert.Null(_store.GetAccountSecretHash(Peer));
        Assert.Equal(1, CountEvents("register_key_switch_refused"));
    }

    [Fact]
    public async Task SameKey_ReRegister_OnTheSameConnection_IsAllowed()
    {
        var hub = NewHub("Bind");
        var secret = RelaySecret.Generate();
        var client = await RegisterOkAsync(hub, Key, secret);

        await client.RegisterAsync(Key, secret);

        await client.ExpectTypeAsync(MessageTypes.PairSync);
        Assert.True(client.IsOpen);
        Assert.Equal(0, CountEvents("register_key_switch_refused"));
    }

    [Fact]
    public async Task ReplacedSession_CannotActAsTheKeyAfterItsClose()
    {
        var hub = NewHub("Bind");
        var secret = RelaySecret.Generate();
        var old = await RegisterOkAsync(hub, Key, secret);
        var replacement = await RegisterOkAsync(hub, Key, secret);
        await old.UntilCloseAsync();

        // Frames the old socket still sends after the relay's close are ignored, including a re-register.
        await old.RegisterAsync(Key, secret);

        Assert.Null(await replacement.NextAsync(Quiet));
        Assert.True(replacement.IsOpen);
        Assert.Equal(1, CountEvents("session_replaced"));
    }

    [Fact]
    public async Task LegacyRegister_OnAHeldKey_StillGetsPairKeyCollision()
    {
        var hub = NewHub("Bind");
        var holder = await RegisterOkAsync(hub, Key, null);
        var second = await ConnectAsync(hub);

        await second.RegisterAsync(Key, null);

        var error = await second.ExpectTypeAsync(MessageTypes.Error);
        Assert.Equal(ErrorCodes.PairKeyCollision, error.GetPayload<ErrorPayload>()!.Code);
        Assert.True(holder.IsOpen);
        Assert.Null(await holder.NextAsync(Quiet));
    }

    [Theory]
    [InlineData("Bind")]
    [InlineData("Off")]
    public async Task RoutedRelayOnlyErrors_AreDropped_OrdinaryOnesStillFlow(string mode)
    {
        var hub = NewHub(mode);
        var target = await RegisterOkAsync(hub, Key, null);
        var sender = await RegisterOkAsync(hub, Peer, null);

        foreach (var code in new[]
                 {
                     ErrorCodes.AuthFailed, ErrorCodes.AuthRequired, ErrorCodes.SessionReplaced,
                     ErrorCodes.PairKeyCollision, ErrorCodes.RateLimited,
                 })
        {
            await sender.SendAsync(MessageTypes.Error, new ErrorPayload { To = Key, Code = code, Message = "forged" });
        }

        Assert.Null(await target.NextAsync(Quiet));

        await sender.SendAsync(MessageTypes.Error, new ErrorPayload { To = Key, Code = ErrorCodes.NotAllowed, Message = "ok" });
        var routed = await target.ExpectTypeAsync(MessageTypes.Error);
        Assert.Equal(ErrorCodes.NotAllowed, routed.GetPayload<ErrorPayload>()!.Code);
    }

    [Fact]
    public async Task SecretlessRegister_OnAClaimedKey_ClosesSilently_AndRecordsOncePerWindow()
    {
        var hub = NewHub("Bind");
        _store.TryCreateAccount(Key, RelaySecret.HashHex(RelaySecret.Generate())!);

        for (var attempt = 0; attempt < 3; attempt++)
        {
            var client = await ConnectAsync(hub);
            await client.RegisterAsync(Key, null);

            var (messages, status, description) = await client.UntilCloseAsync();
            Assert.Empty(messages);
            Assert.Equal(WebSocketCloseStatus.PolicyViolation, status);
            Assert.Equal(ErrorCodes.AuthRequired, description);
            await client.CloseOutputAsync();
        }

        Assert.Equal(1, CountEvents("register_refused"));
    }

    [Fact]
    public async Task WrongSecret_GetsAuthFailed_ThenClose()
    {
        var hub = NewHub("Bind");
        var owner = await RegisterOkAsync(hub, Key, RelaySecret.Generate());
        var intruder = await ConnectAsync(hub);

        await intruder.RegisterAsync(Key, RelaySecret.Generate());

        var (messages, status, description) = await intruder.UntilCloseAsync();
        var error = Assert.Single(messages);
        Assert.Equal(ErrorCodes.AuthFailed, error.GetPayload<ErrorPayload>()!.Code);
        Assert.Null(error.GetPayload<ErrorPayload>()!.To);
        Assert.Equal(WebSocketCloseStatus.PolicyViolation, status);
        Assert.Equal(ErrorCodes.AuthFailed, description);
        Assert.True(owner.IsOpen);
    }

    [Fact]
    public async Task RepeatedWrongSecret_GetsAuthFailedEveryTime_ButIsRecordedOncePerWindow()
    {
        var hub = NewHub("Bind");
        _store.TryCreateAccount(Key, RelaySecret.HashHex(RelaySecret.Generate())!);

        for (var attempt = 0; attempt < 3; attempt++)
        {
            var client = await ConnectAsync(hub);
            await client.RegisterAsync(Key, RelaySecret.Generate());

            var (messages, _, description) = await client.UntilCloseAsync();
            Assert.Equal(ErrorCodes.AuthFailed, Assert.Single(messages).GetPayload<ErrorPayload>()!.Code);
            Assert.Equal(ErrorCodes.AuthFailed, description);
            await client.CloseOutputAsync();
        }

        Assert.Equal(1, CountEvents("register_auth_failed"));
    }

    [Fact]
    public async Task AfterARefusal_TheRelayWaitsForTheClientsClose_AndIgnoresQueuedFrames()
    {
        var hub = NewHub("Enforce");
        var client = await ConnectAsync(hub);

        // The register is refused; the pings queued behind it must not be answered or reset the connection.
        await client.RegisterAsync(Key, null);
        for (var i = 0; i < 3; i++)
            await client.SendAsync(MessageTypes.Ping, new { });

        var (messages, status, description) = await client.UntilCloseAsync();
        Assert.Empty(messages);
        Assert.Equal(WebSocketCloseStatus.PolicyViolation, status);
        Assert.Equal(ErrorCodes.AuthRequired, description);

        await Task.Delay(300);
        Assert.False(client.ServerTask.IsCompleted, "the relay disposed the socket before the client's close");

        await client.CloseOutputAsync();
        await client.ServerTask.WaitAsync(Wait);
    }

    [Fact]
    public async Task AfterARefusal_TheDrainIsBounded()
    {
        var hub = NewHub("Enforce");
        var client = await ConnectAsync(hub);
        await client.RegisterAsync(Key, null);
        await client.UntilCloseAsync();

        // The client never answers the close: the relay gives up after about two seconds.
        var started = DateTime.UtcNow;
        await client.ServerTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(DateTime.UtcNow - started < TimeSpan.FromSeconds(4));
    }

    [Fact]
    public async Task OffMode_IgnoresSecrets_AndKeepsLegacyBehaviour()
    {
        var hub = NewHub("Off");
        // A key claimed earlier (e.g. before a rollback to Off) is still usable without a secret.
        _store.TryCreateAccount(Peer, RelaySecret.HashHex(RelaySecret.Generate())!);
        await RegisterOkAsync(hub, Peer, null);

        // A secret neither binds a free key nor takes over a held one.
        var holder = await RegisterOkAsync(hub, Key, RelaySecret.Generate());
        Assert.Null(_store.GetAccountSecretHash(Key));

        var second = await ConnectAsync(hub);
        await second.RegisterAsync(Key, RelaySecret.Generate());
        var error = await second.ExpectTypeAsync(MessageTypes.Error);
        Assert.Equal(ErrorCodes.PairKeyCollision, error.GetPayload<ErrorPayload>()!.Code);
        Assert.True(holder.IsOpen);
        Assert.Null(await holder.NextAsync(Quiet));

        Assert.Equal(0, CountEvents("register_legacy"));
        Assert.Equal(0, CountEvents("register_bound"));
        Assert.Equal(0, CountEvents("session_replaced"));
    }

    private sealed record Received(Envelope? Envelope, WebSocketCloseStatus? CloseStatus, string? CloseDescription);

    private sealed class TestClient : IDisposable
    {
        private readonly TcpClient _tcp;
        private readonly TcpClient _serverTcp;
        private readonly WebSocket _socket;
        private readonly Channel<Received> _inbox = Channel.CreateUnbounded<Received>();

        private TestClient(TcpClient tcp, TcpClient serverTcp, WebSocket socket, Task serverTask, bool reading)
        {
            _tcp = tcp;
            _serverTcp = serverTcp;
            _socket = socket;
            ServerTask = serverTask;
            if (reading)
                _ = PumpAsync();
        }

        /// <summary>The relay's <see cref="SessionHub.HandleClientAsync"/> for this connection.</summary>
        public Task ServerTask { get; }

        public bool IsOpen => _socket.State == WebSocketState.Open;

        /// <summary>
        /// <paramref name="reading"/> false never reads from the socket, with small buffers on both ends,
        /// so the relay's sends to it soon block: a stuck client.
        /// </summary>
        public static async Task<TestClient> ConnectAsync(SessionHub hub, bool reading, CancellationToken ct)
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            try
            {
                var port = ((IPEndPoint)listener.LocalEndpoint).Port;
                var tcp = new TcpClient { NoDelay = true };
                if (!reading)
                    tcp.ReceiveBufferSize = 4 * 1024;
                var connect = tcp.ConnectAsync(IPAddress.Loopback, port, ct).AsTask();
                var serverTcp = await listener.AcceptTcpClientAsync(ct);
                await connect;
                if (!reading)
                    serverTcp.SendBufferSize = 4 * 1024;

                var serverSocket = WebSocket.CreateFromStream(serverTcp.GetStream(), new WebSocketCreationOptions { IsServer = true });
                var clientSocket = WebSocket.CreateFromStream(tcp.GetStream(), new WebSocketCreationOptions { IsServer = false });
                var serverTask = Task.Run(() => hub.HandleClientAsync(serverSocket, ct), ct);
                return new TestClient(tcp, serverTcp, clientSocket, serverTask, reading);
            }
            finally
            {
                listener.Stop();
            }
        }

        public Task RegisterAsync(string key, string? secret) =>
            SendAsync(MessageTypes.Register, new RegisterPayload { PairingKey = key, Secret = secret });

        public async Task SendAsync(string type, object payload)
        {
            var bytes = Encoding.UTF8.GetBytes(Envelope.Create(type, payload).Serialize());
            await _socket.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
        }

        public async Task CloseOutputAsync()
        {
            if (_socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
                await _socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None);
        }

        /// <summary>Next envelope or close, or null when nothing arrives in time.</summary>
        public async Task<Received?> NextAsync(TimeSpan timeout)
        {
            using var cts = new CancellationTokenSource(timeout);
            try
            {
                return await _inbox.Reader.ReadAsync(cts.Token);
            }
            catch (OperationCanceledException)
            {
                return null;
            }
            catch (ChannelClosedException)
            {
                return null;
            }
        }

        public async Task<Envelope> ExpectTypeAsync(string type)
        {
            var deadline = DateTime.UtcNow + Wait;
            while (DateTime.UtcNow < deadline)
            {
                var next = await NextAsync(deadline - DateTime.UtcNow);
                if (next?.Envelope is { } envelope && envelope.Type == type)
                    return envelope;
                if (next is not null && next.Envelope is null)
                    break;
            }

            throw new Xunit.Sdk.XunitException($"expected {type}");
        }

        public async Task<(List<Envelope> Messages, WebSocketCloseStatus? Status, string? Description)> UntilCloseAsync()
        {
            var messages = new List<Envelope>();
            var deadline = DateTime.UtcNow + Wait;
            while (DateTime.UtcNow < deadline)
            {
                var next = await NextAsync(deadline - DateTime.UtcNow);
                if (next is null)
                    break;
                if (next.Envelope is null)
                    return (messages, next.CloseStatus, next.CloseDescription);
                messages.Add(next.Envelope);
            }

            throw new Xunit.Sdk.XunitException("expected the relay to close the socket");
        }

        private async Task PumpAsync()
        {
            var buffer = new byte[16 * 1024];
            try
            {
                while (true)
                {
                    var result = await _socket.ReceiveAsync(buffer, CancellationToken.None);
                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        await _inbox.Writer.WriteAsync(new Received(null, result.CloseStatus, result.CloseStatusDescription));
                        break;
                    }

                    var json = Encoding.UTF8.GetString(buffer, 0, result.Count);
                    while (!result.EndOfMessage)
                    {
                        result = await _socket.ReceiveAsync(buffer, CancellationToken.None);
                        json += Encoding.UTF8.GetString(buffer, 0, result.Count);
                    }

                    if (Envelope.TryParse(json) is { } envelope)
                        await _inbox.Writer.WriteAsync(new Received(envelope, null, null));
                }
            }
            catch (Exception)
            {
                // Connection dropped without a close frame.
                await _inbox.Writer.WriteAsync(new Received(null, null, null));
            }
            finally
            {
                _inbox.Writer.TryComplete();
            }
        }

        public void Dispose()
        {
            _socket.Dispose();
            _tcp.Dispose();
            _serverTcp.Dispose();
        }
    }
}
