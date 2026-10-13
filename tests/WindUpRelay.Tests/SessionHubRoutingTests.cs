using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using WindUpKey.Protocol;

namespace WindUpRelay.Tests;

/// <summary>Routing integrity across sessions: who may address whom, what <c>from</c> says, and outbox delivery.</summary>
public sealed partial class SessionHubTests
{
    private const string Other = "OTHERCC3";
    private const string Offline = "OFFLINE9";
    private const string Victim = "VICTIM99";

    private long CountOutbox(string? status = null)
    {
        using var conn = new SqliteConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = status is null
            ? "SELECT COUNT(*) FROM outbox;"
            : "SELECT COUNT(*) FROM outbox WHERE status = $s;";
        cmd.Parameters.AddWithValue("$s", status ?? string.Empty);
        return (long)cmd.ExecuteScalar()!;
    }

    private string OutboxStatus(long id)
    {
        using var conn = new SqliteConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT status FROM outbox WHERE id = $id;";
        cmd.Parameters.AddWithValue("$id", id);
        return (string)cmd.ExecuteScalar()!;
    }

    /// <summary>The relay marks a row delivered just after the frame is sent, so the client may see it first.</summary>
    private async Task<string> StatusSoonAsync(long id, string expected)
    {
        var deadline = DateTime.UtcNow + Wait;
        while (OutboxStatus(id) != expected && DateTime.UtcNow < deadline)
            await Task.Delay(20);
        return OutboxStatus(id);
    }

    private long Enqueue(string from, string to, string type, object payload, string? requestId = null) =>
        _store.EnqueueOutbox(requestId, from, to, type, Envelope.Create(type, payload).Serialize(), TimeSpan.FromDays(1));

    /// <summary>Every reply type the relay routes, each addressed to <paramref name="to"/> with a forged <c>from</c>.</summary>
    private static IEnumerable<(string Type, object Payload)> ForgedReplies(string to) =>
    [
        (MessageTypes.PresenceResult, new PresenceResultPayload { RequestId = "r1", From = Victim, To = to, Online = true, StillPaired = false }),
        (MessageTypes.WindResult, new WindResultPayload { RequestId = "r2", From = Victim, To = to, RemainingSeconds = 60 }),
        (MessageTypes.OwnerSettingsResult, new OwnerSettingsResultPayload { RequestId = "r3", From = Victim, To = to }),
        (MessageTypes.OwnerSettingsAck, new OwnerSettingsAckPayload { RequestId = "r4", From = Victim, To = to }),
        (MessageTypes.CallAck, new CallAckPayload { RequestId = "r5", From = Victim, To = to, Status = CallAckStatuses.Busy }),
        (MessageTypes.CallResult, new CallResultPayload { RequestId = "r6", From = Victim, To = to, Status = CallResultStatuses.Failed }),
        (MessageTypes.Error, new ErrorPayload { RequestId = "r7", From = Victim, To = to, Code = ErrorCodes.NotAllowed, Message = "x" }),
    ];

    /// <summary>Every forward that needs a pair, addressed to <paramref name="to"/>, with the requestId it carries (if any).</summary>
    private static IEnumerable<(string Type, object Payload, string? RequestId)> Forwards(string to) =>
    [
        (MessageTypes.Wind, new WindPayload { RequestId = "f1", To = to, Hours = 1 }, "f1"),
        (MessageTypes.Unwind, new UnwindPayload { RequestId = "f2", To = to }, "f2"),
        (MessageTypes.OwnerSettingsQuery, new OwnerSettingsQueryPayload { RequestId = "f3", To = to }, "f3"),
        (MessageTypes.OwnerSettingsUpdate, new OwnerSettingsUpdatePayload { RequestId = "f4", To = to, MaxWindHours = 2 }, "f4"),
        (MessageTypes.Call, new CallPayload { RequestId = "f5", To = to, WorldId = 1, TerritoryId = 1 }, "f5"),
        (MessageTypes.OwnerGrant, new OwnerGrantPayload { To = to }, null),
    ];

    [Fact]
    public async Task RoutedReplies_CarryTheSendersKey_WhateverFromItWrote()
    {
        var hub = NewHub("Bind");
        var target = await RegisterOkAsync(hub, Key, null);
        var sender = await RegisterOkAsync(hub, Peer, null);
        Pair(Key, Peer);

        // 05-D1: a settings result is routed only when it was asked for.
        await target.SendAsync(MessageTypes.OwnerSettingsQuery, new OwnerSettingsQueryPayload { RequestId = "r3", To = Peer });
        await sender.ExpectTypeAsync(MessageTypes.OwnerSettingsQuery);

        foreach (var (type, payload) in ForgedReplies(Key))
        {
            await sender.SendAsync(type, payload);
            var routed = await target.ExpectTypeAsync(type);
            Assert.Equal(Peer, (string?)routed.Payload!["from"]);
            Assert.Equal(Key, (string?)routed.Payload!["to"]);
        }
    }

    [Fact]
    public async Task RoutedReplies_BetweenUnpairedKeys_AreDropped()
    {
        var hub = NewHub("Bind");
        var target = await RegisterOkAsync(hub, Key, null);
        var sender = await RegisterOkAsync(hub, Other, null);
        Pair(Key, Peer);

        foreach (var (type, payload) in ForgedReplies(Key))
            await sender.SendAsync(type, payload);

        Assert.Null(await target.NextAsync(Quiet));
        Assert.Null(await sender.NextAsync(TimeSpan.FromMilliseconds(100)));
        Assert.Equal(1, CountEvents("route_not_paired"));
    }

    [Fact]
    public async Task Forwards_ToAnUnpairedKey_AreRefused_AndNeitherDeliveredNorQueued()
    {
        var hub = NewHub("Bind");
        var sender = await RegisterOkAsync(hub, Key, null);
        var online = await RegisterOkAsync(hub, Other, null);
        Pair(Key, Peer);

        foreach (var to in new[] { Other, Offline })
        {
            foreach (var (type, payload, requestId) in Forwards(to))
            {
                await sender.SendAsync(type, payload);
                if (requestId is null)
                {
                    // ownerGrant has no requestId to answer: dropped silently.
                    Assert.Null(await sender.NextAsync(TimeSpan.FromMilliseconds(200)));
                    continue;
                }

                var error = await sender.ExpectTypeAsync(MessageTypes.Error);
                var refused = error.GetPayload<ErrorPayload>()!;
                Assert.Equal(ErrorCodes.NotAllowed, refused.Code);
                Assert.Equal(requestId, refused.RequestId);
                Assert.Equal(Key, refused.To);
                Assert.Null(refused.From);
            }
        }

        Assert.Null(await online.NextAsync(Quiet));
        Assert.Equal(0, CountOutbox());
        Assert.Equal(0, CountEvents("queue"));
    }

    [Fact]
    public async Task Forwards_ToAPartner_StillFlow_AndQueueWhenOffline()
    {
        var hub = NewHub("Bind");
        var sender = await RegisterOkAsync(hub, Key, null);
        var partner = await RegisterOkAsync(hub, Peer, null);
        Pair(Key, Peer);

        foreach (var (type, payload, _) in Forwards(Peer))
        {
            await sender.SendAsync(type, payload);
            var forwarded = await partner.ExpectTypeAsync(type);
            Assert.Equal(Key, (string?)forwarded.Payload!["from"]);
        }

        const string offlinePartner = "PARTNER4";
        Pair(Key, offlinePartner);
        await sender.SendAsync(MessageTypes.Wind, new WindPayload { RequestId = "q1", To = offlinePartner, Hours = 1 });
        await sender.ExpectTypeAsync(MessageTypes.DeliveryQueued);
        Assert.Equal(1, CountOutbox("pending"));
    }

    [Fact]
    public async Task MessagesToOneself_StillWork()
    {
        var hub = NewHub("Bind");
        var self = await RegisterOkAsync(hub, Key, null);

        await self.SendAsync(MessageTypes.Wind, new WindPayload { RequestId = "s1", To = Key, Hours = 1 });
        var wind = await self.ExpectTypeAsync(MessageTypes.Wind);
        Assert.Equal(Key, wind.GetPayload<WindPayload>()!.From);

        await self.SendAsync(MessageTypes.WindResult, new WindResultPayload { RequestId = "s1", From = Victim, To = Key });
        var result = await self.ExpectTypeAsync(MessageTypes.WindResult);
        Assert.Equal(Key, result.GetPayload<WindResultPayload>()!.From);

        await self.SendAsync(MessageTypes.PresenceQuery, new PresenceQueryPayload { RequestId = "s2", To = Key });
        await self.ExpectTypeAsync(MessageTypes.PresenceQuery);
        var presence = await self.ExpectTypeAsync(MessageTypes.PresenceResult);
        Assert.True(presence.GetPayload<PresenceResultPayload>()!.Online);
        Assert.Equal(0, CountEvents("route_not_paired"));
    }

    [Fact]
    public async Task Presence_ForAnUnpairedKey_LooksExactlyOffline_AndIsNotForwarded()
    {
        var hub = NewHub("Bind");
        var asker = await RegisterOkAsync(hub, Key, null);
        var unpaired = await RegisterOkAsync(hub, Other, null);

        await asker.SendAsync(MessageTypes.PresenceQuery, new PresenceQueryPayload { RequestId = "p", To = Other });
        var forUnpaired = (await asker.ExpectTypeAsync(MessageTypes.PresenceResult)).Payload!;
        await asker.SendAsync(MessageTypes.PresenceQuery, new PresenceQueryPayload { RequestId = "p", To = Offline });
        var forOffline = (await asker.ExpectTypeAsync(MessageTypes.PresenceResult)).Payload!;

        Assert.Null(await unpaired.NextAsync(Quiet));

        // Same fields and values, apart from which key it is about.
        forUnpaired["from"] = "X";
        forOffline["from"] = "X";
        Assert.Equal(forOffline.ToJsonString(), forUnpaired.ToJsonString());
        Assert.False((bool)forUnpaired["online"]!);
        Assert.False(forUnpaired.ContainsKey("stillPaired"));

        // Once paired, the same query says online and is forwarded.
        Pair(Key, Other);
        await asker.SendAsync(MessageTypes.PresenceQuery, new PresenceQueryPayload { RequestId = "p", To = Other });
        Assert.True((await asker.ExpectTypeAsync(MessageTypes.PresenceResult)).GetPayload<PresenceResultPayload>()!.Online);
        Assert.Equal(Key, (await unpaired.ExpectTypeAsync(MessageTypes.PresenceQuery)).GetPayload<PresenceQueryPayload>()!.From);
    }

    [Fact]
    public async Task QueuedMessages_AreRecheckedAtFlush()
    {
        var hub = NewHub("Bind");
        Pair(Key, Peer);

        // Queued before the pair check existed (or on a host that still had the pair): Other isn't paired with Peer.
        var stale = Enqueue(Other, Peer, MessageTypes.Wind, new WindPayload { RequestId = "stale", From = Other, To = Peer, Hours = 1 }, "stale");
        var notice = Enqueue(Other, Peer, MessageTypes.PairRemove, new PairRemovePayload { PeerKey = Other });
        var fresh = Enqueue(Key, Peer, MessageTypes.Wind, new WindPayload { RequestId = "fresh", From = Key, To = Peer, Hours = 1 }, "fresh");

        var peer = await RegisterOkAsync(hub, Peer, null);

        Assert.Equal(MessageTypes.PairRemove, (await peer.NextAsync(Wait))!.Envelope!.Type);
        Assert.Equal("fresh", (await peer.ExpectTypeAsync(MessageTypes.Wind)).GetPayload<WindPayload>()!.RequestId);
        Assert.Null(await peer.NextAsync(Quiet));
        Assert.Equal("expired", OutboxStatus(stale));
        Assert.Equal("delivered", OutboxStatus(notice));
        Assert.Equal("delivered", OutboxStatus(fresh));
        Assert.Equal(1, CountEvents("outbox_drop_unpaired"));
    }

    [Fact]
    public async Task ConsentRows_ForUnpairedPeers_AreIgnored_WithOneEventPerPush()
    {
        var hub = NewHub("Bind");
        var doll = await RegisterOkAsync(hub, Key, null);
        Pair(Key, Peer);
        var now = DateTimeOffset.UtcNow.ToString("O");

        for (var push = 0; push < 2; push++)
        {
            await doll.SendAsync(MessageTypes.DollStatePush, new DollStatePushPayload
            {
                Peers =
                [
                    new PairConsentSyncEntry { PeerKey = Peer, IsOwner = true, UpdatedUtc = now },
                    new PairConsentSyncEntry { PeerKey = Other, IsOwner = true, CanCallMe = true, UpdatedUtc = now },
                    new PairConsentSyncEntry { PeerKey = Offline, CanWindMe = true, UpdatedUtc = now },
                ],
            });
        }

        await Task.Delay(Quiet);
        var row = Assert.Single(_store.GetPairConsentForDoll(Key));
        Assert.Equal(Peer, row.PeerKey);
        Assert.Equal(2, CountEvents("pair_consent_unpaired"));
    }

    [Fact]
    public async Task Stamps_MoreThanFiveMinutesAhead_AreRejected_AndRecordedOncePerWindow()
    {
        var hub = NewHub("Bind");
        var doll = await RegisterOkAsync(hub, Key, null);
        Pair(Key, Peer);
        var ahead = DateTimeOffset.UtcNow.AddMinutes(6).ToString("O");

        for (var push = 0; push < 2; push++)
        {
            await doll.SendAsync(MessageTypes.DollStatePush, new DollStatePushPayload
            {
                ExpiryUtc = DateTimeOffset.UtcNow.AddHours(1).ToString("O"),
                WindUpdatedUtc = ahead,
                Peers = [new PairConsentSyncEntry { PeerKey = Peer, IsOwner = true, UpdatedUtc = ahead }],
            });
        }

        await Task.Delay(Quiet);
        Assert.Empty(_store.GetPairConsentForDoll(Key));
        Assert.Null(_store.GetDollWind(Key));
        Assert.Equal(1, CountEvents("future_stamp_rejected"));

        // Within the allowed skew, it's accepted as before.
        var slightlyAhead = DateTimeOffset.UtcNow.AddMinutes(4).ToString("O");
        await doll.SendAsync(MessageTypes.DollStatePush, new DollStatePushPayload
        {
            WindUpdatedUtc = slightlyAhead,
            ClearWind = true,
            Peers = [new PairConsentSyncEntry { PeerKey = Peer, IsOwner = true, UpdatedUtc = slightlyAhead }],
        });
        await Task.Delay(Quiet);
        Assert.True(Assert.Single(_store.GetPairConsentForDoll(Key)).IsOwner);
        Assert.NotNull(_store.GetDollWind(Key));
    }

    [Fact]
    public async Task PairSync_ListsOnlyEstablishedPairs_AndOnlyTheirConsent()
    {
        var hub = NewHub("Bind");
        var now = DateTimeOffset.UtcNow.ToString("O");

        // Consent rows alone (a removed pair, or rows from before pushes were checked) never make a partner.
        Assert.True(_store.TryMergePairConsent(new PairConsentRow(Key, Other, true, true, true, true, now)));
        var lonely = await ConnectAsync(hub);
        await lonely.RegisterAsync(Key, null);
        var empty = (await lonely.ExpectTypeAsync(MessageTypes.PairSync)).GetPayload<PairSyncPayload>()!;
        Assert.Empty(empty.PeerKeys ?? []);
        Assert.Empty(empty.Peers ?? []);
        lonely.Dispose();

        Pair(Key, Peer);
        Assert.True(_store.TryMergePairConsent(new PairConsentRow(Key, Peer, true, false, true, false, now)));
        var client = await ConnectAsync(hub);
        await client.RegisterAsync(Key, null);
        var sync = (await client.ExpectTypeAsync(MessageTypes.PairSync)).GetPayload<PairSyncPayload>()!;
        Assert.Equal([Peer], sync.PeerKeys);
        var row = Assert.Single(sync.Peers!);
        Assert.Equal(Peer, row.PeerKey);
        Assert.True(row.IsOwner);
    }

    [Fact]
    public async Task AMalformedPayload_IsDropped_WithoutEndingTheSendersSession()
    {
        var hub = NewHub("Bind");
        var sender = await RegisterOkAsync(hub, Key, null);
        var partner = await RegisterOkAsync(hub, Peer, null);
        Pair(Key, Peer);

        // Wrong JSON types in a forward, a reply, a push and a presence query, and a payload that isn't an object.
        static string Frame(string type, string payload) => $$"""{"type":"{{type}}","payload":{{payload}}}""";
        string[] malformed =
        [
            Frame(MessageTypes.Wind, $$"""{"requestId":"m1","to":"{{Peer}}","hours":"lots"}"""),
            Frame(MessageTypes.WindResult, $$$"""{"requestId":"m2","to":{"key":"{{{Peer}}}"}}"""),
            Frame(MessageTypes.DollStatePush, """{"peers":"none"}"""),
            Frame(MessageTypes.PresenceQuery, "7"),
        ];
        foreach (var json in malformed)
            await sender.SendRawAsync(json);

        await sender.SendAsync(MessageTypes.Ping, new { });
        await sender.ExpectTypeAsync(MessageTypes.Pong);
        Assert.True(sender.IsOpen);
        Assert.False(sender.ServerTask.IsCompleted);
        Assert.Null(await partner.NextAsync(Quiet));
        Assert.Equal(1, CountEvents("payload_malformed"));

        // The session still routes normally afterwards.
        await sender.SendAsync(MessageTypes.Wind, new WindPayload { RequestId = "ok", To = Peer, Hours = 1 });
        Assert.Equal("ok", (await partner.ExpectTypeAsync(MessageTypes.Wind)).GetPayload<WindPayload>()!.RequestId);
    }

    [Fact]
    public async Task ARecipientWhoseSocketDiesMidSend_DoesNotEndTheSendersSession()
    {
        var hub = NewHub("Bind");
        var sender = await RegisterOkAsync(hub, Key, null);
        var dying = await RegisterOkAsync(hub, Peer, null);
        Pair(Key, Peer);
        dying.BreakRelayWrites();

        // Queueable: treated as offline and queued.
        await sender.SendAsync(MessageTypes.Wind, new WindPayload { RequestId = "w", To = Peer, Hours = 1 });
        Assert.Equal("w", (await sender.ExpectTypeAsync(MessageTypes.DeliveryQueued)).GetPayload<DeliveryStatusPayload>()!.RequestId);
        Assert.Equal(1, CountOutbox("pending"));

        // Not queueable: dropped (a call says the target is offline).
        await sender.SendAsync(MessageTypes.Call, new CallPayload { RequestId = "c", To = Peer, WorldId = 1, TerritoryId = 1 });
        Assert.Equal(ErrorCodes.TargetOffline, (await sender.ExpectTypeAsync(MessageTypes.Error)).GetPayload<ErrorPayload>()!.Code);

        await sender.SendAsync(MessageTypes.Ping, new { });
        await sender.ExpectTypeAsync(MessageTypes.Pong);
        Assert.True(sender.IsOpen);
        Assert.False(sender.ServerTask.IsCompleted);
        Assert.True(CountEvents("send_failed") >= 1);
    }

    [Fact]
    public async Task QueuedRows_AreMarkedDelivered_OnlyWhenTheSendHappened_AndFlushStopsAtTheFirstFailure()
    {
        var hub = NewHub("Bind");
        Pair(Key, Peer);
        var first = Enqueue(Key, Peer, MessageTypes.Wind, new WindPayload { RequestId = "a", From = Key, To = Peer, Hours = 1 }, "a");
        var second = Enqueue(Key, Peer, MessageTypes.Wind, new WindPayload { RequestId = "b", From = Key, To = Peer, Hours = 1 }, "b");

        // pairSync goes through, the first queued row's send fails, and later sends would work again.
        var secret = RelaySecret.Generate();
        var broken = await ConnectAsync(hub);
        broken.BreakRelayWrites(count: 1, skip: 1);
        await broken.RegisterAsync(Peer, secret);
        await broken.ExpectTypeAsync(MessageTypes.PairSync);
        await Task.Delay(Quiet);

        Assert.Equal("pending", OutboxStatus(first));
        Assert.Equal("pending", OutboxStatus(second));
        Assert.Equal(1, CountEvents("send_failed"));
        Assert.Null(await broken.NextAsync(TimeSpan.FromMilliseconds(100)));

        // Both are delivered, in order, on the next register.
        var peer = await RegisterOkAsync(hub, Peer, secret);
        Assert.Equal("a", (await peer.ExpectTypeAsync(MessageTypes.Wind)).GetPayload<WindPayload>()!.RequestId);
        Assert.Equal("b", (await peer.ExpectTypeAsync(MessageTypes.Wind)).GetPayload<WindPayload>()!.RequestId);
        Assert.Equal("delivered", await StatusSoonAsync(first, "delivered"));
        Assert.Equal("delivered", await StatusSoonAsync(second, "delivered"));
    }

    [Fact]
    public async Task Acks_AreTiedToTheRecipientAndTheOriginalSender()
    {
        var hub = NewHub("Bind");
        Pair(Key, Peer);
        Pair(Key, Other);
        Pair(Peer, Other);
        var row = Enqueue(Key, Peer, MessageTypes.Wind, new WindPayload { RequestId = "rid", From = Key, To = Peer, Hours = 1 }, "rid");
        var key = await RegisterOkAsync(hub, Key, null);
        var other = await RegisterOkAsync(hub, Other, null);
        var peer = await RegisterOkAsync(hub, Peer, null);
        await peer.ExpectTypeAsync(MessageTypes.Wind);
        Assert.Equal("delivered", await StatusSoonAsync(row, "delivered"));

        // Someone else replying with the requestId doesn't ack it.
        await other.SendAsync(MessageTypes.WindResult, new WindResultPayload { RequestId = "rid", To = Key });
        await key.ExpectTypeAsync(MessageTypes.WindResult);
        Assert.Equal("delivered", OutboxStatus(row));

        // Nor does the recipient replying toward a different key.
        await peer.SendAsync(MessageTypes.WindResult, new WindResultPayload { RequestId = "rid", To = Other });
        await other.ExpectTypeAsync(MessageTypes.WindResult);
        Assert.Equal("delivered", OutboxStatus(row));

        // The recipient replying to the sender does.
        await peer.SendAsync(MessageTypes.WindResult, new WindResultPayload { RequestId = "rid", To = Key });
        await key.ExpectTypeAsync(MessageTypes.WindResult);
        Assert.Equal("acked", OutboxStatus(row));
    }

    /// <summary>
    /// How long the send-timeout tests wait, with their 1 s timeout: generous for a loaded machine, yet short of
    /// <see cref="SessionHub.DefaultSendTimeout"/>, so a hub or session that ignored its configured timeout fails.
    /// </summary>
    private static readonly TimeSpan ShortOfTheDefaultSendTimeout = SessionHub.DefaultSendTimeout - TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// A wind for a stuck client. 03-D4 caps every field (a forward can no longer be bigger than a stuck client's
    /// buffers), so the stuck client's writes are stalled instead (<see cref="RegisterStuckAsync"/>).
    /// </summary>
    private static WindPayload StuckWind(string to) => new() { RequestId = "stuck-" + Guid.NewGuid().ToString("N"), To = to, Hours = 1 };

    /// <summary>A registered client that never reads, so the relay's next send to it blocks.</summary>
    private async Task<TestClient> RegisterStuckAsync(SessionHub hub, string key)
    {
        var stuck = await ConnectAsync(hub, reading: false);
        await stuck.RegisterAsync(key, null);
        await Task.Delay(200);
        stuck.StallRelayWrites();
        return stuck;
    }

    [Fact]
    public async Task ARecipientAbortedDuringABlockedSend_DoesNotEndTheSendersSession_AndTheMessageIsQueued()
    {
        // A long send timeout, so it's the abort that ends the blocked send.
        var hub = NewHub("Bind", sendTimeout: TimeSpan.FromSeconds(30));
        var sender = await RegisterOkAsync(hub, Key, null);
        var recipient = await RegisterStuckAsync(hub, Peer);
        Pair(Key, Peer);

        var wind = StuckWind(Peer);
        await sender.SendAsync(MessageTypes.Wind, wind);
        Assert.Null(await sender.NextAsync(Quiet));

        // What the relay does to a stuck replaced session, or what a dropped connection looks like mid-send.
        recipient.AbortRelaySide();

        var queued = await sender.ExpectTypeAsync(MessageTypes.DeliveryQueued);
        Assert.Equal(wind.RequestId, queued.GetPayload<DeliveryStatusPayload>()!.RequestId);
        Assert.Equal(1, CountOutbox("pending"));
        Assert.Equal(1, CountEvents("send_failed"));

        await sender.SendAsync(MessageTypes.Ping, new { });
        await sender.ExpectTypeAsync(MessageTypes.Pong);
        Assert.True(sender.IsOpen);
        Assert.False(sender.ServerTask.IsCompleted);
    }

    [Fact]
    public async Task TheSenderDisconnectingMidSend_DoesNotAbortTheRecipient()
    {
        var hub = NewHub("Bind", sendTimeout: TimeSpan.FromSeconds(30));
        using var senderGone = new CancellationTokenSource();
        var sender = await ConnectAsync(hub, requestAborted: senderGone.Token);
        await sender.RegisterAsync(Key, null);
        await sender.ExpectTypeAsync(MessageTypes.PairSync);
        var recipient = await RegisterStuckAsync(hub, Peer);
        Pair(Key, Peer);

        var wind = StuckWind(Peer);
        await sender.SendAsync(MessageTypes.Wind, wind);
        await Task.Delay(Quiet);

        // The sender's connection goes away (its RequestAborted fires) while the relay is still sending its wind.
        senderGone.Cancel();
        await Task.Delay(Quiet);
        Assert.False(recipient.ServerTask.IsCompleted, "the sender's disconnect aborted the recipient's socket");

        // The recipient reads again: the whole wind arrives, and its connection still works.
        recipient.StartReading();
        var received = await recipient.ExpectTypeAsync(MessageTypes.Wind);
        Assert.Equal(wind.RequestId, received.GetPayload<WindPayload>()!.RequestId);
        await recipient.SendAsync(MessageTypes.Ping, new { });
        await recipient.ExpectTypeAsync(MessageTypes.Pong);
        Assert.True(recipient.IsOpen);
    }

    [Fact]
    public async Task ARecipientThatStopsReading_StallsTheSenderOnlyUntilTheSendTimeout()
    {
        var timeout = TimeSpan.FromSeconds(1);
        var hub = NewHub("Bind", sendTimeout: timeout);
        var sender = await RegisterOkAsync(hub, Key, null);
        await RegisterStuckAsync(hub, Peer);
        Pair(Key, Peer);

        var started = DateTime.UtcNow;
        await sender.SendAsync(MessageTypes.Wind, StuckWind(Peer));

        // Counted as offline after the timeout: queued, and the sender's loop goes on. The wait leaves a loaded
        // machine plenty of time, but ends short of the 5 s default, so it's this hub's timeout that applied.
        await sender.ExpectTypeAsync(MessageTypes.DeliveryQueued, ShortOfTheDefaultSendTimeout);
        var elapsed = DateTime.UtcNow - started;
        Assert.True(elapsed >= timeout - TimeSpan.FromMilliseconds(100), $"queued after {elapsed}, before the timeout");
        Assert.True(elapsed < ShortOfTheDefaultSendTimeout, $"the sender stalled for {elapsed}");
        Assert.Equal(1, CountOutbox("pending"));
        Assert.Equal(1, CountEvents("send_failed"));

        await sender.SendAsync(MessageTypes.Ping, new { });
        await sender.ExpectTypeAsync(MessageTypes.Pong);
        Assert.True(sender.IsOpen);
    }

    [Fact]
    public async Task ASessionThatDoesNotReadItsOwnReplies_IsAborted_AndLaterSendsToItAreFastAndOffline()
    {
        // 02-D25: own-loop sends are bounded too, so a wedged session can't hold its send lock and stall every
        // partner that sends to it, each time.
        var timeout = TimeSpan.FromSeconds(1);
        var hub = NewHub("Bind", sendTimeout: timeout);
        var sender = await RegisterOkAsync(hub, Key, null);
        var wedged = await RegisterStuckAsync(hub, Peer);
        Pair(Key, Peer);

        // The wedged client provokes a reply to itself (a not_allowed for its requestId), which it doesn't read.
        await wedged.SendAsync(MessageTypes.Wind, StuckWind(Offline));

        // Aborted after this hub's timeout, not the 5 s default.
        var ended = await Task.WhenAny(wedged.ServerTask, Task.Delay(ShortOfTheDefaultSendTimeout));
        Assert.True(ReferenceEquals(ended, wedged.ServerTask), "the wedged session was not aborted");

        for (var i = 0; i < 3; i++)
        {
            // A stalled send would wait out the whole send timeout first.
            var started = DateTime.UtcNow;
            await sender.SendAsync(MessageTypes.Wind, new WindPayload { RequestId = "w" + i, To = Peer, Hours = 1 });
            await sender.ExpectTypeAsync(MessageTypes.DeliveryQueued);
            var elapsed = DateTime.UtcNow - started;
            Assert.True(elapsed < timeout, $"send {i} to the wedged session stalled for {elapsed}");
        }

        Assert.Equal(3, CountOutbox("pending"));
        Assert.True(sender.IsOpen);
    }

    [Fact]
    public async Task PairRemove_IsDeliveredLiveAlways_ButQueuedOnlyWhenItRemovedSomething()
    {
        var hub = NewHub("Bind");
        var key = await RegisterOkAsync(hub, Key, null);
        var other = await RegisterOkAsync(hub, Other, null);

        // Live: delivered even without a pair.
        await key.SendAsync(MessageTypes.PairRemove, new PairRemovePayload { PeerKey = Other });
        Assert.Equal(Key, (await other.ExpectTypeAsync(MessageTypes.PairRemove)).GetPayload<PairRemovePayload>()!.PeerKey);

        // Offline and nothing to remove: not queued.
        await key.SendAsync(MessageTypes.PairRemove, new PairRemovePayload { PeerKey = Offline });
        await key.SendAsync(MessageTypes.Ping, new { });
        await key.ExpectTypeAsync(MessageTypes.Pong);
        Assert.Equal(0, CountOutbox());

        // Offline, removing an established pair: queued once; a repeat removes nothing and isn't queued again.
        Pair(Key, Offline);
        await key.SendAsync(MessageTypes.PairRemove, new PairRemovePayload { PeerKey = Offline });
        await key.SendAsync(MessageTypes.PairRemove, new PairRemovePayload { PeerKey = Offline });
        await key.SendAsync(MessageTypes.Ping, new { });
        await key.ExpectTypeAsync(MessageTypes.Pong);
        Assert.Equal(1, CountOutbox("pending"));

        // Offline, removing only the peer's pending submit toward the sender: queued.
        _store.UpsertPairSubmit(Victim, Key);
        await key.SendAsync(MessageTypes.PairRemove, new PairRemovePayload { PeerKey = Victim });
        await key.SendAsync(MessageTypes.Ping, new { });
        await key.ExpectTypeAsync(MessageTypes.Pong);
        Assert.Equal(2, CountOutbox("pending"));
        Assert.False(_store.HasPairSubmit(Victim, Key));
    }

    [Fact]
    public async Task PairRemove_OfTheSendersOwnSubmit_IsNotQueued_SoALoopCannotFillAStrangersOutbox()
    {
        // 02-D25: an unpaired sender submitting the victim's key and removing it again, over and over.
        var hub = NewHub("Bind");
        var attacker = await RegisterOkAsync(hub, Key, null);
        for (var i = 0; i < 20; i++)
        {
            await attacker.SendAsync(MessageTypes.PairSubmit, new PairSubmitPayload { RequestId = "s" + i, MyKey = Key, TheirKey = Victim });
            await attacker.SendAsync(MessageTypes.PairRemove, new PairRemovePayload { PeerKey = Victim });
        }

        await attacker.SendAsync(MessageTypes.Ping, new { });
        await attacker.ExpectTypeAsync(MessageTypes.Pong);
        Assert.Equal(0, CountOutbox("pending"));
        Assert.False(_store.HasPairSubmit(Key, Victim));
    }

    [Fact]
    public async Task PairRemove_IsQueuedOncePerSenderAndPeer_WhileOneIsPending()
    {
        var hub = NewHub("Bind");
        var key = await RegisterOkAsync(hub, Key, null);

        // Each removal really removes something (the pair, re-made in between), but one pending notice is enough.
        for (var i = 0; i < 5; i++)
        {
            Pair(Key, Offline);
            await key.SendAsync(MessageTypes.PairRemove, new PairRemovePayload { PeerKey = Offline });
            await key.SendAsync(MessageTypes.Ping, new { });
            await key.ExpectTypeAsync(MessageTypes.Pong);
        }

        Assert.Equal(1, CountOutbox("pending"));

        var offline = await ConnectAsync(hub);
        await offline.RegisterAsync(Offline, null);
        await offline.ExpectTypeAsync(MessageTypes.PairSync);
        Assert.Equal(Key, (await offline.ExpectTypeAsync(MessageTypes.PairRemove)).GetPayload<PairRemovePayload>()!.PeerKey);
        Assert.Null(await offline.NextAsync(Quiet));
    }

    [Fact]
    public async Task OwnerRevoked_BetweenUnpairedKeys_IsDroppedSilently_LiveAndQueued()
    {
        // 02-D28: a refused one gets no error and leaves only a throttled event.
        var hub = NewHub("Bind");
        var key = await RegisterOkAsync(hub, Key, null);
        var other = await RegisterOkAsync(hub, Other, null);
        Pair(Key, Peer);

        await key.SendAsync(MessageTypes.OwnerRevoked, new OwnerRevokedPayload { To = Other });
        await key.SendAsync(MessageTypes.OwnerRevoked, new OwnerRevokedPayload { To = Other });
        await key.SendAsync(MessageTypes.OwnerRevoked, new OwnerRevokedPayload { To = Offline });
        await key.SendAsync(MessageTypes.Ping, new { });

        // Nothing comes back before the pong: no error for the refusals.
        Assert.Equal(MessageTypes.Pong, (await key.NextAsync(Wait))?.Envelope?.Type);
        Assert.Null(await other.NextAsync(Quiet));
        Assert.Equal(0, CountOutbox());
        Assert.Equal(1, CountEvents("route_not_paired"));
    }

    [Fact]
    public async Task OwnerRevoked_BetweenPairedKeys_IsDeliveredLive_AndToOneself()
    {
        var hub = NewHub("Bind");
        var key = await RegisterOkAsync(hub, Key, null);
        var peer = await RegisterOkAsync(hub, Peer, null);
        Pair(Key, Peer);

        await key.SendAsync(MessageTypes.OwnerRevoked, new OwnerRevokedPayload { From = Victim, To = Peer });
        Assert.Equal(Key, (await peer.ExpectTypeAsync(MessageTypes.OwnerRevoked)).GetPayload<OwnerRevokedPayload>()!.From);

        // The debug self-pair.
        await key.SendAsync(MessageTypes.OwnerRevoked, new OwnerRevokedPayload { To = Key });
        Assert.Equal(Key, (await key.ExpectTypeAsync(MessageTypes.OwnerRevoked)).GetPayload<OwnerRevokedPayload>()!.From);
        Assert.Equal(0, CountOutbox());
        Assert.Equal(0, CountEvents("route_not_paired"));
    }

    [Fact]
    public async Task OwnerRevoked_ToAnOfflinePartner_IsQueuedOnce_AndKeptByTheRemovalThatFollows()
    {
        var hub = NewHub("Bind");
        var key = await RegisterOkAsync(hub, Key, null);
        Pair(Key, Offline);

        // 02-D29: one pending copy per sender and recipient, however often it's sent.
        for (var i = 0; i < 3; i++)
            await key.SendAsync(MessageTypes.OwnerRevoked, new OwnerRevokedPayload { To = Offline });
        await key.SendAsync(MessageTypes.Ping, new { });
        await key.ExpectTypeAsync(MessageTypes.Pong);
        Assert.Equal(1, CountOutbox("pending"));
        Assert.Equal(1, CountEvents("queue"));

        // The client sends it just before its pairRemove, which keeps it.
        await key.SendAsync(MessageTypes.PairRemove, new PairRemovePayload { PeerKey = Offline });
        await key.SendAsync(MessageTypes.Ping, new { });
        await key.ExpectTypeAsync(MessageTypes.Pong);
        Assert.Equal(2, CountOutbox("pending"));

        var offline = await ConnectAsync(hub);
        await offline.RegisterAsync(Offline, null);
        await offline.ExpectTypeAsync(MessageTypes.PairSync);
        Assert.Equal(Key, (await offline.ExpectTypeAsync(MessageTypes.OwnerRevoked)).GetPayload<OwnerRevokedPayload>()!.From);
        Assert.Equal(Key, (await offline.ExpectTypeAsync(MessageTypes.PairRemove)).GetPayload<PairRemovePayload>()!.PeerKey);
        Assert.Null(await offline.NextAsync(Quiet));
    }

    [Fact]
    public async Task GrantRevokeGrantRevoke_ToAnOfflineOwner_LeavesItNotAnOwner()
    {
        // 02-D31: a revoke drops the grants queued before it, so the one-copy revoke can't be followed by an older grant.
        var hub = NewHub("Bind");
        var doll = await RegisterOkAsync(hub, Key, null);
        Pair(Key, Offline);

        await doll.SendAsync(MessageTypes.OwnerGrant, new OwnerGrantPayload { To = Offline });
        await doll.SendAsync(MessageTypes.OwnerRevoked, new OwnerRevokedPayload { To = Offline });
        await doll.SendAsync(MessageTypes.OwnerGrant, new OwnerGrantPayload { To = Offline });
        await doll.SendAsync(MessageTypes.OwnerRevoked, new OwnerRevokedPayload { To = Offline });
        await doll.SendAsync(MessageTypes.Ping, new { });
        await doll.ExpectTypeAsync(MessageTypes.Pong);
        Assert.Equal(1, CountOutbox());

        // The owner gets only the revoke: it ends up not an owner.
        Assert.Equal([MessageTypes.OwnerRevoked], await DeliveredOnRegisterAsync(hub, Offline));

        // Only the grants before a revoke go: grant, revoke, grant to another offline owner arrives as revoke, grant.
        Pair(Key, Other);
        await doll.SendAsync(MessageTypes.OwnerGrant, new OwnerGrantPayload { To = Other });
        await doll.SendAsync(MessageTypes.OwnerRevoked, new OwnerRevokedPayload { To = Other });
        await doll.SendAsync(MessageTypes.OwnerGrant, new OwnerGrantPayload { To = Other });
        await doll.SendAsync(MessageTypes.Ping, new { });
        await doll.ExpectTypeAsync(MessageTypes.Pong);
        Assert.Equal([MessageTypes.OwnerRevoked, MessageTypes.OwnerGrant], await DeliveredOnRegisterAsync(hub, Other));
    }

    /// <summary>The types <paramref name="key"/> is sent after its <c>pairSync</c> when it registers, in order.</summary>
    private async Task<List<string>> DeliveredOnRegisterAsync(SessionHub hub, string key)
    {
        var client = await RegisterOkAsync(hub, key, null);
        var delivered = new List<string>();
        while (await client.NextAsync(Quiet) is { Envelope: { } envelope })
            delivered.Add(envelope.Type);
        return delivered;
    }
}
