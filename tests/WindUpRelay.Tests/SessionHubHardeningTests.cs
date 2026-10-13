using System.Net.WebSockets;
using System.Text;
using Microsoft.Data.Sqlite;
using WindUpKey.Protocol;

namespace WindUpRelay.Tests;

/// <summary>Relay hardening (issue 05) across real loopback sessions: requested settings results (05-D1) and closing (05-D4).</summary>
public sealed partial class SessionHubTests
{
    private const string Owner = "OWNERAA1";
    private const string Doll = "DOLLBBB2";
    private const string Third = "THIRDCC3";

    private sealed class SettingsTime : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = DateTimeOffset.UtcNow;
        public override DateTimeOffset GetUtcNow() => Now;
    }

    private static OwnerSettingsResultPayload SettingsResult(string requestId, string to, int emotes = 3) => new()
    {
        RequestId = requestId,
        To = to,
        MaxWindHours = 12,
        Emotes = Enumerable.Range(1, emotes).Select(i => new OwnerEmoteInfo { Id = (ushort)i, Name = "Emote " + i }).ToList(),
    };

    /// <summary>An owner and a doll, online and paired.</summary>
    private async Task<(TestClient Owner, TestClient Doll)> OwnerAndDollAsync(SessionHub hub)
    {
        var owner = await RegisterOkAsync(hub, Owner, RelaySecret.Generate());
        var doll = await RegisterOkAsync(hub, Doll, RelaySecret.Generate());
        Pair(Owner, Doll);
        return (owner, doll);
    }

    /// <summary>The owner asks; the doll gets the query live (the relay recorded it as delivered).</summary>
    private static async Task<string> QueryLiveAsync(TestClient owner, TestClient doll)
    {
        var requestId = "q-" + Guid.NewGuid().ToString("N");
        await owner.SendAsync(MessageTypes.OwnerSettingsQuery, new OwnerSettingsQueryPayload { RequestId = requestId, To = Doll });
        var query = await doll.ExpectTypeAsync(MessageTypes.OwnerSettingsQuery);
        Assert.Equal(requestId, query.GetPayload<OwnerSettingsQueryPayload>()!.RequestId);
        return requestId;
    }

    private string OutboxStatusOf(string requestId)
    {
        using var conn = new SqliteConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT status FROM outbox WHERE request_id = $rid;";
        cmd.Parameters.AddWithValue("$rid", requestId);
        return (string)cmd.ExecuteScalar()!;
    }

    /// <summary>Moves the outbox row's delivery back by <paramref name="age"/>, as if it was delivered that long ago.</summary>
    private void DeliveredAgo(string requestId, TimeSpan age)
    {
        using var conn = new SqliteConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE outbox SET delivered_utc = $utc WHERE request_id = $rid AND status = 'delivered';";
        cmd.Parameters.AddWithValue("$rid", requestId);
        cmd.Parameters.AddWithValue("$utc", DateTimeOffset.UtcNow.Subtract(age).ToString("O"));
        Assert.Equal(1, cmd.ExecuteNonQuery());
    }

    // ---- 05-D1: only requested ownerSettingsResult replies are routed ----

    [Fact]
    public async Task AnUnsolicitedLargeSettingsResult_IsDropped_WithAThrottledEvent()
    {
        var hub = NewHub("Bind");
        var (owner, doll) = await OwnerAndDollAsync(hub);

        // About 60 KiB once re-serialised: under every 03 limit, so only the 05-D1 gate can stop it.
        var big = SettingsResult("never-asked", Owner, emotes: 512);
        foreach (var emote in big.Emotes)
            emote.Name = new string('n', 59) + "ééééé";
        var size = Encoding.UTF8.GetByteCount(Envelope.Create(MessageTypes.OwnerSettingsResult, big).Serialize());
        Assert.InRange(size, 55 * 1024, WireLimits.RoutedMessageBytes - 1024);

        await doll.SendAsync(MessageTypes.OwnerSettingsResult, big);
        await doll.SendAsync(MessageTypes.OwnerSettingsResult, big);

        Assert.Null(await owner.NextAsync(Quiet));
        Assert.Null(await doll.NextAsync(TimeSpan.FromMilliseconds(100)));
        Assert.Equal(1, CountEvents("reply_unrequested"));
        Assert.Equal(0, CountEvents("routed_too_big"));
        Assert.True(doll.IsOpen);
    }

    [Fact]
    public async Task ALiveQuery_GetsItsResultOnce_AndARepeatIsDropped()
    {
        var hub = NewHub("Bind");
        var (owner, doll) = await OwnerAndDollAsync(hub);
        var requestId = await QueryLiveAsync(owner, doll);

        await doll.SendAsync(MessageTypes.OwnerSettingsResult, SettingsResult(requestId, Owner));
        var result = await owner.ExpectTypeAsync(MessageTypes.OwnerSettingsResult);
        Assert.Equal(requestId, result.GetPayload<OwnerSettingsResultPayload>()!.RequestId);
        Assert.Equal(Doll, result.GetPayload<OwnerSettingsResultPayload>()!.From);

        await doll.SendAsync(MessageTypes.OwnerSettingsResult, SettingsResult(requestId, Owner));
        Assert.Null(await owner.NextAsync(Quiet));
        Assert.Equal(1, CountEvents("reply_unrequested"));
    }

    [Fact]
    public async Task ADollAnsweringBeforeTheRelaysSendReturns_StillGetsItsResultThrough()
    {
        // The entry is made before the query is sent, not after: a doll may answer while that send is still returning.
        var hub = NewHub("Bind");
        var (owner, doll) = await OwnerAndDollAsync(hub);
        doll.SlowRelayWriteReturns(TimeSpan.FromMilliseconds(800));

        await owner.SendAsync(MessageTypes.OwnerSettingsQuery, new OwnerSettingsQueryPayload { RequestId = "q-fast", To = Doll });
        await doll.ExpectTypeAsync(MessageTypes.OwnerSettingsQuery);
        await doll.SendAsync(MessageTypes.OwnerSettingsResult, SettingsResult("q-fast", Owner));

        var result = await owner.ExpectTypeAsync(MessageTypes.OwnerSettingsResult);
        Assert.Equal("q-fast", result.GetPayload<OwnerSettingsResultPayload>()!.RequestId);
    }

    [Fact]
    public async Task AWrongRequestId_AThirdPartnerReusingTheId_OrTheWrongDirection_IsDropped()
    {
        var hub = NewHub("Bind");
        var (owner, doll) = await OwnerAndDollAsync(hub);
        var third = await RegisterOkAsync(hub, Third, RelaySecret.Generate());
        Pair(Third, Owner);
        Pair(Third, Doll);
        var requestId = await QueryLiveAsync(owner, doll);

        await doll.SendAsync(MessageTypes.OwnerSettingsResult, SettingsResult("q-other", Owner));
        await third.SendAsync(MessageTypes.OwnerSettingsResult, SettingsResult(requestId, Owner));
        await owner.SendAsync(MessageTypes.OwnerSettingsResult, SettingsResult(requestId, Doll));
        await doll.SendAsync(MessageTypes.OwnerSettingsResult, SettingsResult(requestId, Third));
        Assert.Null(await owner.NextAsync(Quiet));
        Assert.Null(await doll.NextAsync(TimeSpan.FromMilliseconds(100)));
        Assert.Null(await third.NextAsync(TimeSpan.FromMilliseconds(100)));

        // None of them used up the doll's own answer.
        await doll.SendAsync(MessageTypes.OwnerSettingsResult, SettingsResult(requestId, Owner));
        await owner.ExpectTypeAsync(MessageTypes.OwnerSettingsResult);
    }

    [Fact]
    public async Task AQueuedQuery_GetsItsResultAfterTheFlush_AndARepeatAfterTheAckIsDropped()
    {
        var hub = NewHub("Bind");
        var owner = await RegisterOkAsync(hub, Owner, RelaySecret.Generate());
        Pair(Owner, Doll);
        var requestId = "q-" + Guid.NewGuid().ToString("N");
        await owner.SendAsync(MessageTypes.OwnerSettingsQuery, new OwnerSettingsQueryPayload { RequestId = requestId, To = Doll });
        await owner.ExpectTypeAsync(MessageTypes.DeliveryQueued);

        var doll = await ConnectAsync(hub);
        await doll.RegisterAsync(Doll, RelaySecret.Generate());
        await doll.ExpectTypeAsync(MessageTypes.OwnerSettingsQuery);
        await owner.ExpectTypeAsync(MessageTypes.DeliveryDelivered);
        Assert.Equal("delivered", OutboxStatusOf(requestId));

        await doll.SendAsync(MessageTypes.OwnerSettingsResult, SettingsResult(requestId, Owner));
        await owner.ExpectTypeAsync(MessageTypes.OwnerSettingsResult);
        Assert.Equal("acked", OutboxStatusOf(requestId));

        await doll.SendAsync(MessageTypes.OwnerSettingsResult, SettingsResult(requestId, Owner));
        Assert.Null(await owner.NextAsync(Quiet));
        Assert.Equal(1, CountEvents("reply_unrequested"));
    }

    [Fact]
    public void OnlyADeliveredQueryRow_FromTheOwnerToTheDoll_CountsAsRequested()
    {
        long Queue(string requestId, string from, string to, string type) =>
            _store.EnqueueOutbox(requestId, from, to, type, "{}", TimeSpan.FromDays(1));

        Queue("q-pending", Owner, Doll, MessageTypes.OwnerSettingsQuery);
        _store.MarkDelivered(Queue("q-delivered", Owner, Doll, MessageTypes.OwnerSettingsQuery));
        _store.MarkDelivered(Queue("q-acked", Owner, Doll, MessageTypes.OwnerSettingsQuery));
        _store.MarkAckedByRequestId("q-acked", ackerKey: Doll, senderKey: Owner);
        Assert.True(_store.MarkExpired(Queue("q-expired", Owner, Doll, MessageTypes.OwnerSettingsQuery)));
        _store.MarkDelivered(Queue("q-update", Owner, Doll, MessageTypes.OwnerSettingsUpdate));

        Assert.True(_store.HasDeliveredRequest("q-delivered", Owner, Doll, MessageTypes.OwnerSettingsQuery));
        Assert.False(_store.HasDeliveredRequest("q-delivered", Doll, Owner, MessageTypes.OwnerSettingsQuery));
        Assert.False(_store.HasDeliveredRequest("q-delivered", Owner, Third, MessageTypes.OwnerSettingsQuery));
        Assert.False(_store.HasDeliveredRequest("q-pending", Owner, Doll, MessageTypes.OwnerSettingsQuery));
        Assert.False(_store.HasDeliveredRequest("q-acked", Owner, Doll, MessageTypes.OwnerSettingsQuery));
        Assert.False(_store.HasDeliveredRequest("q-expired", Owner, Doll, MessageTypes.OwnerSettingsQuery));
        Assert.False(_store.HasDeliveredRequest("q-update", Owner, Doll, MessageTypes.OwnerSettingsQuery));
        Assert.False(_store.HasDeliveredRequest("", Owner, Doll, MessageTypes.OwnerSettingsQuery));
    }

    [Fact]
    public void ADeliveredQueryRow_CountsForTenMinutesOnly()
    {
        // 05-D13: a doll answers a flushed query as soon as it reads it, so an old delivery is no credit for a result.
        _store.MarkDelivered(_store.EnqueueOutbox("q-recent", Owner, Doll, MessageTypes.OwnerSettingsQuery, "{}", TimeSpan.FromDays(14)));
        _store.MarkDelivered(_store.EnqueueOutbox("q-old", Owner, Doll, MessageTypes.OwnerSettingsQuery, "{}", TimeSpan.FromDays(14)));
        DeliveredAgo("q-recent", RelayStore.DeliveredRequestWindow - TimeSpan.FromMinutes(1));
        DeliveredAgo("q-old", RelayStore.DeliveredRequestWindow + TimeSpan.FromMinutes(1));

        Assert.True(_store.HasDeliveredRequest("q-recent", Owner, Doll, MessageTypes.OwnerSettingsQuery));
        Assert.False(_store.HasDeliveredRequest("q-old", Owner, Doll, MessageTypes.OwnerSettingsQuery));
        Assert.Equal(TimeSpan.FromMinutes(10), RelayStore.DeliveredRequestWindow);
    }

    [Fact]
    public async Task AResultToAQueryDeliveredFromTheOutboxOverTenMinutesAgo_IsDropped()
    {
        var hub = NewHub("Bind");
        var owner = await RegisterOkAsync(hub, Owner, RelaySecret.Generate());
        Pair(Owner, Doll);
        var requestId = "q-" + Guid.NewGuid().ToString("N");
        await owner.SendAsync(MessageTypes.OwnerSettingsQuery, new OwnerSettingsQueryPayload { RequestId = requestId, To = Doll });
        await owner.ExpectTypeAsync(MessageTypes.DeliveryQueued);

        var doll = await ConnectAsync(hub);
        await doll.RegisterAsync(Doll, RelaySecret.Generate());
        await doll.ExpectTypeAsync(MessageTypes.OwnerSettingsQuery);
        await owner.ExpectTypeAsync(MessageTypes.DeliveryDelivered);
        DeliveredAgo(requestId, RelayStore.DeliveredRequestWindow + TimeSpan.FromSeconds(30));

        await doll.SendAsync(MessageTypes.OwnerSettingsResult, SettingsResult(requestId, Owner));

        Assert.Null(await owner.NextAsync(Quiet));
        Assert.Equal(1, CountEvents("reply_unrequested"));
        Assert.Equal("delivered", OutboxStatusOf(requestId));
    }

    [Fact]
    public async Task AResultAfterTheTwoMinuteWindow_IsDropped()
    {
        var time = new SettingsTime();
        var hub = NewHub("Bind", settingsQueries: new RequestedReplies(time));
        var (owner, doll) = await OwnerAndDollAsync(hub);
        var requestId = await QueryLiveAsync(owner, doll);

        time.Now += RequestedReplies.Ttl + TimeSpan.FromSeconds(1);
        await doll.SendAsync(MessageTypes.OwnerSettingsResult, SettingsResult(requestId, Owner));

        Assert.Null(await owner.NextAsync(Quiet));
        Assert.Equal(1, CountEvents("reply_unrequested"));
    }

    [Fact]
    public async Task AnUnsolicitedWindResult_IsStillDelivered()
    {
        var hub = NewHub("Bind");
        var (owner, doll) = await OwnerAndDollAsync(hub);

        // A windResult can legitimately come days after its queued wind; only settings results are gated.
        await doll.SendAsync(MessageTypes.WindResult, new WindResultPayload { RequestId = "w-days-ago", To = Owner, RemainingSeconds = 3600 });

        var result = await owner.ExpectTypeAsync(MessageTypes.WindResult);
        Assert.Equal("w-days-ago", result.GetPayload<WindResultPayload>()!.RequestId);
        Assert.Equal(0, CountEvents("reply_unrequested"));
    }

    [Fact]
    public async Task ASettingsQueryToOneself_StillGetsItsResult()
    {
        // The debug self-pair: the owner is also the doll.
        var hub = NewHub("Bind");
        var self = await RegisterOkAsync(hub, Owner, RelaySecret.Generate());
        await self.SendAsync(MessageTypes.OwnerSettingsQuery, new OwnerSettingsQueryPayload { RequestId = "q-self", To = Owner });
        await self.ExpectTypeAsync(MessageTypes.OwnerSettingsQuery);

        await self.SendAsync(MessageTypes.OwnerSettingsResult, SettingsResult("q-self", Owner));
        await self.ExpectTypeAsync(MessageTypes.OwnerSettingsResult);
    }

    // ---- 05-D10: a lost race gets its own reason ----

    [Fact]
    public async Task ALostRaceWithNoAccountOnReRead_IsRefusedLikeTheLimit_ButRecordedAsABindRace()
    {
        // The insert finds a row but the re-read finds no hash (a row whose hash isn't text stands in for an account reset
        // between the two), so the re-decision would bind with no account.
        using (var conn = new SqliteConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "INSERT INTO accounts (pairing_key, secret_hash, created_utc, last_seen_utc) VALUES ($k, X'00', 'x', 'x');";
            cmd.Parameters.AddWithValue("$k", Key);
            cmd.ExecuteNonQuery();
        }

        var hub = NewHub("Bind", firstBinds: new FirstBindLimiter(1, 20));
        var client = await ConnectAsync(hub, firstBindKey: "203.0.113.7");
        await client.RegisterAsync(Key, RelaySecret.Generate());

        // The same safe refusal and close as the limit, but its own event.
        var (messages, status, description) = await client.UntilCloseAsync();
        Assert.Empty(messages);
        Assert.Equal(SessionHub.BindRateLimitedClose, status);
        Assert.Equal(WireCloseReasons.BindRateLimited, description);
        Assert.Equal(1, CountEvents("bind_race"));
        Assert.Equal(0, CountEvents("bind_rate_limited"));
        Assert.Equal(0, CountEvents("register"));
        Assert.Equal(0, hub.OnlineCount);

        // Its slot went back: this address may still create its one account.
        var next = await ConnectAsync(hub, firstBindKey: "203.0.113.7");
        await next.RegisterAsync(Peer, RelaySecret.Generate());
        await next.ExpectTypeAsync(MessageTypes.PairSync);
    }

    [Fact]
    public async Task OverTheLimit_TheRefusalIsRecordedAsTheLimit_NotABindRace()
    {
        var hub = NewHub("Bind", firstBinds: new FirstBindLimiter(1, 20));
        var first = await ConnectAsync(hub, firstBindKey: "203.0.113.7");
        await first.RegisterAsync(Key, RelaySecret.Generate());
        await first.ExpectTypeAsync(MessageTypes.PairSync);

        var second = await ConnectAsync(hub, firstBindKey: "203.0.113.7");
        await second.RegisterAsync(Peer, RelaySecret.Generate());
        var (_, status, description) = await second.UntilCloseAsync();

        Assert.Equal(SessionHub.BindRateLimitedClose, status);
        Assert.Equal(WireCloseReasons.BindRateLimited, description);
        Assert.Equal(1, CountEvents("bind_rate_limited"));
        Assert.Equal(0, CountEvents("bind_race"));
        Assert.Null(_store.GetAccountSecretHash(Peer));
    }

    // ---- 05-D4: the close handshake ----

    [Fact]
    public async Task AClientsClose_IsAnswered_AndTheSessionEnds()
    {
        var hub = NewHub("Bind");
        var client = await RegisterOkAsync(hub, Key, RelaySecret.Generate());

        await client.CloseOutputAsync();

        // A close frame, not a dropped connection (which the pump reports with no status).
        var next = await client.NextAsync(Wait);
        Assert.NotNull(next);
        Assert.Null(next.Envelope);
        Assert.Equal(WebSocketCloseStatus.NormalClosure, next.CloseStatus);
        await client.ServerTask.WaitAsync(Wait);
    }

    [Fact]
    public async Task ATakeoverOfASessionThatStoppedReading_EndsTheOldSessionAfterTheDrainWindow()
    {
        var hub = NewHub("Bind");
        var secret = RelaySecret.Generate();
        var silent = await ConnectAsync(hub, reading: false);
        await silent.RegisterAsync(Key, secret);
        await Task.Delay(200);

        var started = DateTime.UtcNow;
        await RegisterOkAsync(hub, Key, secret);

        // It never answers the relay's close: aborted after the 2 s drain window instead of blocking in its receive.
        await silent.ServerTask.WaitAsync(TimeSpan.FromSeconds(6));
        var elapsed = DateTime.UtcNow - started;
        Assert.True(elapsed >= TimeSpan.FromSeconds(1.5), $"aborted after {elapsed}, before the drain window");
    }

    [Fact]
    public async Task TheConnectionGoingAway_WhileThePeerIsSilent_EndsTheSessionInBoundedTime()
    {
        var hub = NewHub("Bind", sendTimeout: TimeSpan.FromSeconds(1));
        using var requestAborted = new CancellationTokenSource();
        var silent = await ConnectAsync(hub, reading: false, requestAborted: requestAborted.Token);
        await silent.RegisterAsync(Key, null);
        await RegisterStuckAsync(hub, Peer);
        Pair(Key, Peer);

        // The silent session's loop is busy in a send to its stuck partner when its RequestAborted fires, so it leaves
        // the loop with its socket still open, and the relay closes it. The peer never answers that close.
        await silent.SendAsync(MessageTypes.Wind, StuckWind(Peer));
        await Task.Delay(300);
        requestAborted.Cancel();

        await silent.ServerTask.WaitAsync(TimeSpan.FromSeconds(6));
    }
}
