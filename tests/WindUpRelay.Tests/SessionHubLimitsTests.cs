using System.Net.WebSockets;
using System.Text.Encodings.Web;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using WindUpKey.Protocol;

namespace WindUpRelay.Tests;

/// <summary>Message-size and resource limits (issue 03) across real loopback sessions.</summary>
public sealed partial class SessionHubTests
{
    /// <summary>A ping of exactly <paramref name="bytes"/> UTF-8 bytes (padded with a field the relay ignores).</summary>
    private static string PaddedPing(int bytes)
    {
        const string prefix = "{\"type\":\"ping\",\"payload\":{},\"pad\":\"";
        return prefix + new string('x', bytes - prefix.Length - 2) + "\"}";
    }

    private long CountPending(string to, string? type = null)
    {
        using var conn = new SqliteConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = type is null
            ? "SELECT COUNT(*) FROM outbox WHERE to_key = $to AND status = 'pending';"
            : "SELECT COUNT(*) FROM outbox WHERE to_key = $to AND status = 'pending' AND msg_type = $type;";
        cmd.Parameters.AddWithValue("$to", to);
        cmd.Parameters.AddWithValue("$type", type ?? string.Empty);
        return (long)cmd.ExecuteScalar()!;
    }

    private List<string?> EventMsgTypes(string kind)
    {
        using var conn = new SqliteConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT msg_type FROM relay_events WHERE kind = $k;";
        cmd.Parameters.AddWithValue("$k", kind);
        using var reader = cmd.ExecuteReader();
        var list = new List<string?>();
        while (reader.Read())
            list.Add(reader.IsDBNull(0) ? null : reader.GetString(0));
        return list;
    }

    /// <summary>Seeds pending rows as an older relay could have queued them (no limits), in one transaction.</summary>
    private void SeedOutbox(string from, string to, string type, int count, Func<int, object> payload)
    {
        using var conn = new SqliteConnection($"Data Source={_dbPath}");
        conn.Open();
        using var tx = conn.BeginTransaction();
        var now = DateTimeOffset.UtcNow;
        for (var i = 0; i < count; i++)
        {
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText =
                """
                INSERT INTO outbox (request_id, to_key, from_key, msg_type, envelope_json, created_utc, expires_utc, status)
                VALUES (NULL, $to, $from, $type, $json, $now, $exp, 'pending');
                """;
            cmd.Parameters.AddWithValue("$to", to);
            cmd.Parameters.AddWithValue("$from", from);
            cmd.Parameters.AddWithValue("$type", type);
            cmd.Parameters.AddWithValue("$json", Envelope.Create(type, payload(i)).Serialize());
            cmd.Parameters.AddWithValue("$now", now.ToString("O"));
            cmd.Parameters.AddWithValue("$exp", now.AddDays(1).ToString("O"));
            cmd.ExecuteNonQuery();
        }

        tx.Commit();
    }

    /// <summary>An envelope as a client could write it, without escaping <c>&lt;</c>, <c>&amp;</c>, <c>+</c> or non-ASCII.</summary>
    private static string Relaxed(string type, object payload)
    {
        var options = new JsonSerializerOptions(Envelope.JsonOptions) { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };
        return JsonSerializer.Serialize(new { type, payload = (object)payload }, options);
    }

    // ---- 03-D2: message limits ----

    [Fact]
    public async Task AfterRegister_UpToTheLimitIsRead_OneByteMoreIsClosedWith1009()
    {
        var hub = NewHub("Bind");
        var client = await RegisterOkAsync(hub, Key, null);

        await client.SendRawAsync(PaddedPing(10 * 1024));
        await client.ExpectTypeAsync(MessageTypes.Pong);
        await client.SendRawAsync(PaddedPing(WireLimits.RelayMessageBytes));
        await client.ExpectTypeAsync(MessageTypes.Pong);

        await client.SendRawAsync(PaddedPing(WireLimits.RelayMessageBytes + 1));
        var (messages, status, description) = await client.UntilCloseAsync();
        Assert.Empty(messages);
        Assert.Equal(WebSocketCloseStatus.MessageTooBig, status);
        Assert.Equal(WireCloseReasons.MessageTooBig, description);
        Assert.Equal(1, CountEvents("message_too_big"));
    }

    [Fact]
    public async Task BeforeRegister_UpTo4KiBIsRead_OneByteMoreIsClosedWith1009()
    {
        var hub = NewHub("Bind");
        var client = await ConnectAsync(hub);

        await client.SendRawAsync(PaddedPing(WireLimits.PreRegisterMessageBytes));
        await client.ExpectTypeAsync(MessageTypes.Pong);

        await client.SendRawAsync(PaddedPing(WireLimits.PreRegisterMessageBytes + 1));
        var (_, status, description) = await client.UntilCloseAsync();
        Assert.Equal(WebSocketCloseStatus.MessageTooBig, status);
        Assert.Equal(WireCloseReasons.MessageTooBig, description);

        // 03-D7: unregistered connections share one throttled record.
        var second = await ConnectAsync(hub);
        await second.SendRawAsync(PaddedPing(WireLimits.PreRegisterMessageBytes + 1));
        Assert.Equal(WireCloseReasons.MessageTooBig, (await second.UntilCloseAsync()).Description);
        Assert.Equal(1, CountEvents("message_too_big"));
    }

    [Fact]
    public async Task ADisplacedSession_KeepsTheLimitOfARegisteredOne()
    {
        // A displaced session has no PairingKey any more, but it did register: it isn't held to the 4 KiB limit.
        var hub = NewHub("Bind");
        var secret = RelaySecret.Generate();
        var old = await ConnectAsync(hub, reading: false);
        await old.RegisterAsync(Key, secret);
        await Task.Delay(200);
        old.StallRelayWrites(); // so its close stays stuck and its socket open for a while
        await RegisterOkAsync(hub, Key, secret);

        // The limit is chosen when a read starts: one message the relay doesn't answer (its sends to the old socket are
        // stuck) lets the next read start after the takeover.
        await old.SendAsync(MessageTypes.Pong, new { });
        await Task.Delay(200);
        await old.SendRawAsync(PaddedPing(10 * 1024));
        await Task.Delay(Quiet);

        Assert.Equal(0, CountEvents("message_too_big"));
        Assert.Equal(1, CountEvents("session_replaced"));
    }

    [Fact]
    public async Task ABinaryMessage_IsClosedWith1003()
    {
        var hub = NewHub("Bind");
        var client = await RegisterOkAsync(hub, Key, null);

        await client.SendBinaryAsync([1, 2, 3]);

        var (_, status, description) = await client.UntilCloseAsync();
        Assert.Equal(WebSocketCloseStatus.InvalidMessageType, status);
        Assert.Equal(WireCloseReasons.BinaryNotSupported, description);
    }

    // ---- 03-D3: before register ----

    [Fact]
    public async Task ASilentConnection_IsClosedAtTheRegisterDeadline_AndDroppedIfItDoesNotAnswer()
    {
        var hub = NewHub("Bind", registerTimeout: TimeSpan.FromMilliseconds(300));
        var client = await ConnectAsync(hub);

        var (messages, status, description) = await client.UntilCloseAsync();
        Assert.Empty(messages);
        Assert.Equal(WebSocketCloseStatus.PolicyViolation, status);
        Assert.Equal(WireCloseReasons.RegisterTimeout, description);

        // The client never answers the close: aborted after the drain window.
        await client.ServerTask.WaitAsync(TimeSpan.FromSeconds(6));
        Assert.Equal(1, CountEvents("register_timeout"));
    }

    [Fact]
    public async Task ARegisteredSession_OutlivesTheRegisterDeadline()
    {
        var hub = NewHub("Bind", registerTimeout: TimeSpan.FromMilliseconds(300));
        var client = await RegisterOkAsync(hub, Key, null);

        await Task.Delay(TimeSpan.FromSeconds(1));

        await client.SendAsync(MessageTypes.Ping, new { });
        await client.ExpectTypeAsync(MessageTypes.Pong);
        Assert.True(client.IsOpen);
        Assert.Equal(0, CountEvents("register_timeout"));
    }

    [Fact]
    public async Task ANinthMessageBeforeRegister_IsClosedWithRegisterRequired()
    {
        var hub = NewHub("Bind");
        var client = await ConnectAsync(hub);

        for (var i = 0; i < WireLimits.PreRegisterMessages; i++)
        {
            await client.SendAsync(MessageTypes.Ping, new { });
            await client.ExpectTypeAsync(MessageTypes.Pong);
        }

        await client.SendAsync(MessageTypes.Ping, new { });
        var (messages, status, description) = await client.UntilCloseAsync();
        Assert.Empty(messages);
        Assert.Equal(WebSocketCloseStatus.PolicyViolation, status);
        Assert.Equal(WireCloseReasons.RegisterRequired, description);
    }

    [Fact]
    public async Task ARegisterAsTheEighthMessage_IsStillAccepted()
    {
        var hub = NewHub("Bind");
        var client = await ConnectAsync(hub);
        for (var i = 0; i < WireLimits.PreRegisterMessages - 1; i++)
            await client.SendAsync(MessageTypes.Ping, new { });

        await client.RegisterAsync(Key, null);

        await client.ExpectTypeAsync(MessageTypes.PairSync);
        for (var i = 0; i < 20; i++)
            await client.SendAsync(MessageTypes.Ping, new { });
        await client.ExpectTypeAsync(MessageTypes.Pong);
        Assert.True(client.IsOpen);
    }

    // ---- 03-D4: field limits ----

    [Fact]
    public async Task ARequestIdOverTheLimit_IsNeverEchoed()
    {
        var hub = NewHub("Bind");
        var sender = await RegisterOkAsync(hub, Key, null);
        var longId = new string('r', 1000);

        // Would be not_allowed (unpaired) or bad_request (no hours), both echoing the requestId.
        await sender.SendAsync(MessageTypes.Wind, new WindPayload { RequestId = longId, To = Other, Hours = 1 });
        await sender.SendAsync(MessageTypes.Wind, new WindPayload { RequestId = longId, To = Other, Hours = 0 });
        await sender.SendAsync(MessageTypes.PairSubmit, new PairSubmitPayload { RequestId = longId, MyKey = Key, TheirKey = Key });

        Assert.Null(await sender.NextAsync(Quiet));
        Assert.Equal(1, CountEvents("payload_malformed"));

        // The same with a requestId within the limit is answered as before.
        await sender.SendAsync(MessageTypes.Wind, new WindPayload { RequestId = "ok", To = Other, Hours = 1 });
        Assert.Equal("ok", (await sender.ExpectTypeAsync(MessageTypes.Error)).GetPayload<ErrorPayload>()!.RequestId);
    }

    [Fact]
    public async Task AnOversizedIdentity_IsNeitherForwardedNorQueued()
    {
        var hub = NewHub("Bind");
        var doll = await RegisterOkAsync(hub, Key, null);
        var owner = await RegisterOkAsync(hub, Peer, null);
        Pair(Key, Peer);
        Pair(Key, Offline);
        var oversized = new string('n', WireLimits.ShortField + 1);

        await doll.SendAsync(MessageTypes.OwnerGrant, new OwnerGrantPayload { To = Peer, Identity = oversized });
        await doll.SendAsync(MessageTypes.OwnerGrant, new OwnerGrantPayload { To = Offline, Identity = oversized });
        Assert.Null(await owner.NextAsync(Quiet));
        Assert.Equal(0, CountOutbox());

        var label = new string('n', WireLimits.ShortField);
        await doll.SendAsync(MessageTypes.OwnerGrant, new OwnerGrantPayload { To = Peer, Identity = label });
        Assert.Equal(label, (await owner.ExpectTypeAsync(MessageTypes.OwnerGrant)).GetPayload<OwnerGrantPayload>()!.Identity);
    }

    [Fact]
    public async Task ARoutedRelayOnlyError_IsStillDroppedSilently_BeforeTheFieldLimits()
    {
        var hub = NewHub("Bind");
        var target = await RegisterOkAsync(hub, Key, null);
        var sender = await RegisterOkAsync(hub, Peer, null);
        Pair(Key, Peer);

        await sender.SendAsync(MessageTypes.Error, new ErrorPayload { To = Key, Code = ErrorCodes.SessionReplaced, Message = new string('m', 5000) });

        Assert.Null(await target.NextAsync(Quiet));
        Assert.Equal(0, CountEvents("payload_malformed"));
    }

    // ---- 03-D5: routed messages are checked after re-serialisation ----

    [Fact]
    public async Task AnErrorOfTwelveThousandAngleBrackets_IsNotDelivered()
    {
        var hub = NewHub("Bind");
        var target = await RegisterOkAsync(hub, Key, null);
        var sender = await RegisterOkAsync(hub, Peer, null);
        Pair(Key, Peer);

        // Sent unescaped, about 12 KB in; escaped by the relay it would be about 72 KB. 03-D4's 256-character message
        // limit already drops it (so the 03-D5 check is shown with an emote list below).
        await sender.SendRawAsync(Relaxed(MessageTypes.Error, new ErrorPayload { To = Key, Code = ErrorCodes.NotAllowed, Message = new string('<', 12_000) }));

        Assert.Null(await target.NextAsync(Quiet));
        Assert.True(sender.IsOpen);
        Assert.Equal(1, CountEvents("payload_malformed"));
    }

    [Fact]
    public async Task AReplyThatGrowsPastTheRoutedLimitWhenEscaped_IsDropped()
    {
        var hub = NewHub("Bind");
        var owner = await RegisterOkAsync(hub, Key, null);
        var doll = await RegisterOkAsync(hub, Peer, null);
        Pair(Key, Peer);

        // Sent unescaped: about 45 KB in, within every field limit. The relay's encoder writes each '<' as six bytes.
        static string Result(string requestId, char c)
        {
            var payload = new OwnerSettingsResultPayload
            {
                RequestId = requestId,
                To = Key,
                Emotes = Enumerable.Range(0, WireLimits.ListEntries)
                    .Select(i => new OwnerEmoteInfo { Id = (ushort)i, Name = new string(c, WireLimits.ShortField) }).ToList(),
            };
            var json = Relaxed(MessageTypes.OwnerSettingsResult, payload);
            Assert.True(json.Length < WireLimits.RelayMessageBytes, $"{json.Length} bytes");
            return json;
        }

        // 05-D1: both were asked for, so it's only the size that can stop the first.
        foreach (var requestId in new[] { "amplified", "plain" })
        {
            await owner.SendAsync(MessageTypes.OwnerSettingsQuery, new OwnerSettingsQueryPayload { RequestId = requestId, To = Peer });
            await doll.ExpectTypeAsync(MessageTypes.OwnerSettingsQuery);
        }

        await doll.SendRawAsync(Result("amplified", '<'));
        Assert.Null(await owner.NextAsync(Quiet));
        Assert.Equal(1, CountEvents("routed_too_big"));
        Assert.True(doll.IsOpen);

        await doll.SendRawAsync(Result("plain", 'a'));
        var routed = await owner.ExpectTypeAsync(MessageTypes.OwnerSettingsResult);
        Assert.Equal("plain", routed.GetPayload<OwnerSettingsResultPayload>()!.RequestId);
    }

    [Fact]
    public async Task ARowQueuedByAnOlderRelay_TooBigToRouteNow_IsExpiredAtFlush()
    {
        var hub = NewHub("Bind");
        Pair(Key, Peer);
        var big = _store.EnqueueOutbox("big", Key, Peer, MessageTypes.Wind,
            Envelope.Create(MessageTypes.Wind, new WindPayload { RequestId = new string('w', 100_000), From = Key, To = Peer, Hours = 1 }).Serialize(),
            TimeSpan.FromDays(1));
        Enqueue(Key, Peer, MessageTypes.Wind, new WindPayload { RequestId = "small", From = Key, To = Peer, Hours = 1 }, "small");

        var peer = await RegisterOkAsync(hub, Peer, null);

        Assert.Equal("small", (await peer.ExpectTypeAsync(MessageTypes.Wind)).GetPayload<WindPayload>()!.RequestId);
        Assert.Equal("expired", OutboxStatus(big));
        Assert.Equal(1, CountEvents("routed_too_big"));
    }

    // ---- 03-D6: outbox ----

    [Fact]
    public async Task AFullQueue_RefusesTheNewestWithQueueFull_ButRemovalAndPairingNoticesStillQueue()
    {
        var hub = NewHub("Bind");
        for (var s = 0; s < 10; s++)
            SeedOutbox($"SENDER{s:D2}", Offline, MessageTypes.Wind, 100, i => new WindPayload { RequestId = $"s{i}", To = Offline, Hours = 1 });
        var sender = await RegisterOkAsync(hub, Key, null);
        Pair(Key, Offline);

        await sender.SendAsync(MessageTypes.Wind, new WindPayload { RequestId = "full", To = Offline, Hours = 1 });
        var error = (await sender.ExpectTypeAsync(MessageTypes.Error)).GetPayload<ErrorPayload>()!;
        Assert.Equal(ErrorCodes.QueueFull, error.Code);
        Assert.Equal("full", error.RequestId);
        Assert.Equal(Key, error.To);
        Assert.Equal(WireLimits.OutboxPerRecipient, CountPending(Offline));
        Assert.Equal(1, CountEvents("queue_full"));

        await sender.SendAsync(MessageTypes.OwnerRevoked, new OwnerRevokedPayload { To = Offline });
        await sender.SendAsync(MessageTypes.PairRemove, new PairRemovePayload { PeerKey = Offline });
        await sender.SendAsync(MessageTypes.Ping, new { });
        await sender.ExpectTypeAsync(MessageTypes.Pong);
        _store.UpsertPairSubmit(Offline, Key);
        await sender.SendAsync(MessageTypes.PairSubmit, new PairSubmitPayload { RequestId = "p", MyKey = Key, TheirKey = Offline });
        await sender.ExpectTypeAsync(MessageTypes.PairEstablished);

        Assert.Equal(1, CountPending(Offline, MessageTypes.OwnerRevoked));
        Assert.Equal(1, CountPending(Offline, MessageTypes.PairRemove));
        Assert.Equal(1, CountPending(Offline, MessageTypes.PairEstablished));
    }

    [Fact]
    public async Task ASendersQuota_RefusesItsHundredAndFirstMessage()
    {
        var hub = NewHub("Bind");
        SeedOutbox(Key, Offline, MessageTypes.Wind, WireLimits.OutboxPerSender - 1, i => new WindPayload { RequestId = $"s{i}", To = Offline, Hours = 1 });
        var sender = await RegisterOkAsync(hub, Key, null);
        Pair(Key, Offline);

        await sender.SendAsync(MessageTypes.Wind, new WindPayload { RequestId = "last", To = Offline, Hours = 1 });
        Assert.Equal("last", (await sender.ExpectTypeAsync(MessageTypes.DeliveryQueued)).GetPayload<DeliveryStatusPayload>()!.RequestId);
        await sender.SendAsync(MessageTypes.Unwind, new UnwindPayload { RequestId = "over", To = Offline });
        Assert.Equal(ErrorCodes.QueueFull, (await sender.ExpectTypeAsync(MessageTypes.Error)).GetPayload<ErrorPayload>()!.Code);
        Assert.Equal(WireLimits.OutboxPerSender, CountPending(Offline));
    }

    [Fact]
    public async Task GrantsToAnOfflineOwner_AreQueuedOncePerDollAndOwner()
    {
        var hub = NewHub("Bind");
        var doll = await RegisterOkAsync(hub, Key, null);
        Pair(Key, Offline);

        // A doll re-sends its grants on every connect.
        for (var i = 0; i < 3; i++)
            await doll.SendAsync(MessageTypes.OwnerGrant, new OwnerGrantPayload { To = Offline, Identity = "Doll@World" });
        await doll.SendAsync(MessageTypes.Ping, new { });
        await doll.ExpectTypeAsync(MessageTypes.Pong);

        Assert.Equal(1, CountPending(Offline, MessageTypes.OwnerGrant));
    }

    [Fact]
    public async Task TheFlush_DeliversMoreThanOneBatch_InOrder()
    {
        var hub = NewHub("Bind");
        Pair(Key, Peer);
        Pair(Other, Peer);
        SeedOutbox(Key, Peer, MessageTypes.Wind, 70, i => new WindPayload { RequestId = $"k{i:D3}", From = Key, To = Peer, Hours = 1 });
        SeedOutbox(Other, Peer, MessageTypes.Wind, 70, i => new WindPayload { RequestId = $"o{i:D3}", From = Other, To = Peer, Hours = 1 });

        var peer = await RegisterOkAsync(hub, Peer, null);

        var expected = Enumerable.Range(0, 70).Select(i => $"k{i:D3}").Concat(Enumerable.Range(0, 70).Select(i => $"o{i:D3}")).ToList();
        foreach (var id in expected)
            Assert.Equal(id, (await peer.ExpectTypeAsync(MessageTypes.Wind)).GetPayload<WindPayload>()!.RequestId);
        Assert.Equal(0, CountPending(Peer));
    }

    // ---- 03-D7 / 03-D8: rate limit and events ----

    [Fact]
    public async Task ReRegisterSpam_IsRateLimited_AndRecordedOncePerWindow()
    {
        const int perMinute = 10;
        var hub = NewHub("Bind", maxMessagesPerMinute: perMinute);
        var client = await RegisterOkAsync(hub, Key, null);

        // Three times the limit: even split across a minute boundary, one window overflows.
        for (var i = 0; i < 3 * perMinute; i++)
            await client.RegisterAsync(Key, null);

        var error = await client.ExpectTypeAsync(MessageTypes.Error, TimeSpan.FromSeconds(10));
        Assert.Equal(ErrorCodes.RateLimited, error.GetPayload<ErrorPayload>()!.Code);
        await client.SendAsync(MessageTypes.Ping, new { });
        await client.ExpectTypeAsync(MessageTypes.Pong, TimeSpan.FromSeconds(10));
        Assert.True(client.IsOpen);
        Assert.Equal(1, CountEvents("rate_limited"));
        Assert.Equal(MessageTypes.Register, Assert.Single(EventMsgTypes("rate_limited")));
    }

    [Fact]
    public async Task AnUnknownMessageType_IsRecordedAsUnknown()
    {
        const int perMinute = 10;
        var hub = NewHub("Bind", maxMessagesPerMinute: perMinute);
        var client = await RegisterOkAsync(hub, Key, null);

        for (var i = 0; i < 3 * perMinute; i++)
            await client.SendRawAsync("""{"type":"<script>made-up","payload":{}}""");

        await client.ExpectTypeAsync(MessageTypes.Error, TimeSpan.FromSeconds(10));
        Assert.Equal(SessionHub.UnknownMsgType, Assert.Single(EventMsgTypes("rate_limited")));
    }

    // ---- 03-D16: the relay closes as soon as a message crosses its limit ----

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AMessageThatNeverEnds_IsClosedWith1009_AndTheSessionEndsAfterTheDrainWindow(bool registered)
    {
        var hub = NewHub("Bind");
        var client = registered ? await RegisterOkAsync(hub, Key, null) : await ConnectAsync(hub);
        using var stop = new CancellationTokenSource();

        var sending = client.SendEndlessAsync(WebSocketMessageType.Text, stop.Token);

        var (messages, status, description) = await client.UntilCloseAsync();
        Assert.Empty(messages);
        Assert.Equal(WebSocketCloseStatus.MessageTooBig, status);
        Assert.Equal(WireCloseReasons.MessageTooBig, description);

        // The drain reads and ignores the rest until the peer's close or its window, then the session ends, although
        // the client is still sending.
        await client.ServerTask.WaitAsync(TimeSpan.FromSeconds(6));
        await stop.CancelAsync();
        await sending.WaitAsync(TimeSpan.FromSeconds(6));
        Assert.Equal(1, CountEvents("message_too_big"));
    }

    [Fact]
    public async Task ABinaryMessageThatNeverEnds_IsClosedWith1003()
    {
        var hub = NewHub("Bind");
        var client = await RegisterOkAsync(hub, Key, null);
        using var stop = new CancellationTokenSource();

        var sending = client.SendEndlessAsync(WebSocketMessageType.Binary, stop.Token);

        var (_, status, description) = await client.UntilCloseAsync();
        Assert.Equal(WebSocketCloseStatus.InvalidMessageType, status);
        Assert.Equal(WireCloseReasons.BinaryNotSupported, description);
        await client.ServerTask.WaitAsync(TimeSpan.FromSeconds(6));
        await stop.CancelAsync();
        await sending.WaitAsync(TimeSpan.FromSeconds(6));
    }

    // ---- 03-D18: polish ----

    [Fact]
    public async Task AWindRequestRefusedWithQueueFull_DoesNotUseUpTheCooldown()
    {
        var hub = NewHub("Bind");
        for (var s = 0; s < 10; s++)
            SeedOutbox($"SENDER{s:D2}", Offline, MessageTypes.Wind, 100, i => new WindPayload { RequestId = $"s{i}", To = Offline, Hours = 1 });
        var sender = await RegisterOkAsync(hub, Key, null);
        Pair(Key, Offline);

        await sender.SendAsync(MessageTypes.WindRequest, new WindRequestPayload { RequestId = "full", To = Offline, PercentRemaining = 10 });
        var refused = (await sender.ExpectTypeAsync(MessageTypes.Error)).GetPayload<ErrorPayload>()!;
        Assert.Equal(ErrorCodes.QueueFull, refused.Code);
        Assert.Equal("full", refused.RequestId);

        // Once there's room, the next request is queued, not refused with the cooldown, and that one does use it up.
        using (var conn = new SqliteConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "DELETE FROM outbox WHERE id = (SELECT MIN(id) FROM outbox);";
            cmd.ExecuteNonQuery();
        }

        await sender.SendAsync(MessageTypes.WindRequest, new WindRequestPayload { RequestId = "room", To = Offline, PercentRemaining = 10 });
        Assert.Equal("room", (await sender.ExpectTypeAsync(MessageTypes.DeliveryQueued)).GetPayload<DeliveryStatusPayload>()!.RequestId);
        await sender.SendAsync(MessageTypes.WindRequest, new WindRequestPayload { RequestId = "again", To = Offline, PercentRemaining = 10 });
        Assert.Equal(ErrorCodes.WindRequestCooldown, (await sender.ExpectTypeAsync(MessageTypes.Error)).GetPayload<ErrorPayload>()!.Code);
    }

    [Fact]
    public void TheRegisterDeadlineAndARegister_ClaimAConnectionOnlyOnce()
    {
        var (server, client) = InMemorySockets.CreatePair();
        using (server)
        using (client)
        {
            // The deadline first: a register that was already read stops before its auth decision.
            var late = new ClientSession(server, SessionHub.DefaultSendTimeout);
            Assert.True(late.TryExpireRegister());
            Assert.False(late.TryBeginRegister());

            // A register first: the deadline leaves it alone; one that fails hands the connection back.
            var failed = new ClientSession(server, SessionHub.DefaultSendTimeout);
            Assert.True(failed.TryBeginRegister());
            Assert.False(failed.TryExpireRegister());
            failed.EndFailedRegister();
            Assert.True(failed.TryExpireRegister());

            // One that registered keeps it, and may register the same key again.
            var registered = new ClientSession(server, SessionHub.DefaultSendTimeout);
            Assert.True(registered.TryBeginRegister());
            registered.MarkRegistered(Key);
            registered.EndFailedRegister();
            Assert.False(registered.TryExpireRegister());
            Assert.True(registered.TryBeginRegister());
        }
    }

    [Fact]
    public async Task ARegisterInFlightAtTheDeadline_IsWaitedFor_AndTheRegisteredSessionIsNeverClosedByIt()
    {
        var hub = NewHub("Bind", registerTimeout: TimeSpan.FromMilliseconds(300));
        var client = await ConnectAsync(hub);

        // Hold the database's write lock, so the register blocks in its auth decision (creating the account) until
        // well past the deadline.
        using (var conn = new SqliteConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using (var begin = conn.CreateCommand())
            {
                begin.CommandText = "BEGIN IMMEDIATE;";
                begin.ExecuteNonQuery();
            }

            await client.RegisterAsync(Key, RelaySecret.Generate());
            await Task.Delay(TimeSpan.FromSeconds(1));
            Assert.Null(await client.NextAsync(TimeSpan.Zero));

            using var rollback = conn.CreateCommand();
            rollback.CommandText = "ROLLBACK;";
            rollback.ExecuteNonQuery();
        }

        await client.ExpectTypeAsync(MessageTypes.PairSync);
        await Task.Delay(Quiet);
        await client.SendAsync(MessageTypes.Ping, new { });
        await client.ExpectTypeAsync(MessageTypes.Pong);
        Assert.True(client.IsOpen);
        Assert.Equal(0, CountEvents("register_timeout"));
    }

    [Fact]
    public async Task ALegacyRegisterRefusedWithACollision_LeavesTheConnectionToTheDeadline()
    {
        var hub = NewHub("Bind", registerTimeout: TimeSpan.FromMilliseconds(500));
        await RegisterOkAsync(hub, Key, null);
        var client = await ConnectAsync(hub);

        await client.RegisterAsync(Key, null);
        Assert.Equal(ErrorCodes.PairKeyCollision, (await client.ExpectTypeAsync(MessageTypes.Error)).GetPayload<ErrorPayload>()!.Code);

        var (_, status, description) = await client.UntilCloseAsync();
        Assert.Equal(WebSocketCloseStatus.PolicyViolation, status);
        Assert.Equal(WireCloseReasons.RegisterTimeout, description);
    }

    [Fact]
    public async Task ABlankTo_IsDroppedAsMalformed_AndAMissingPayloadIsStillABadRequest()
    {
        // 03-D4 drops a blank `to` before any handler, so the handlers' own blank checks were removed (03-D18).
        var hub = NewHub("Bind");
        var sender = await RegisterOkAsync(hub, Key, null);

        await sender.SendAsync(MessageTypes.WindRequest, new WindRequestPayload { RequestId = "blank", To = " ", PercentRemaining = 10 });
        await sender.SendAsync(MessageTypes.Call, new CallPayload { RequestId = "blank", To = "", WorldId = 1, TerritoryId = 1 });
        Assert.Null(await sender.NextAsync(Quiet));
        Assert.Equal(1, CountEvents("payload_malformed"));

        await sender.SendRawAsync("""{"type":"windRequest","payload":null}""");
        Assert.Equal(ErrorCodes.BadRequest, (await sender.ExpectTypeAsync(MessageTypes.Error)).GetPayload<ErrorPayload>()!.Code);
    }

    [Fact]
    public async Task ASendFailureInALaterFlushBatch_StopsTheFlush_AndTheRestStaysPending()
    {
        var hub = NewHub("Bind");
        Pair(Key, Peer);
        SeedOutbox(Key, Peer, MessageTypes.Wind, 120, i => new WindPayload { RequestId = $"k{i:D3}", From = Key, To = Peer, Hours = 1 });

        // pairSync and the first 60 rows (all of the first batch, and some of the second) go through; the 61st fails.
        var broken = await ConnectAsync(hub);
        broken.BreakRelayWrites(count: 1, skip: 1 + 60);
        await broken.RegisterAsync(Peer, null);
        await broken.ExpectTypeAsync(MessageTypes.PairSync);
        for (var i = 0; i < 60; i++)
            Assert.Equal($"k{i:D3}", (await broken.ExpectTypeAsync(MessageTypes.Wind)).GetPayload<WindPayload>()!.RequestId);
        await Task.Delay(Quiet);

        Assert.Null(await broken.NextAsync(TimeSpan.FromMilliseconds(100)));
        Assert.Equal(60, CountPending(Peer));
        Assert.Equal(1, CountEvents("send_failed"));
    }
}
