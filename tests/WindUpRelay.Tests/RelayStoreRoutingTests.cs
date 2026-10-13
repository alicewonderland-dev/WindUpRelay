using System.Globalization;
using Microsoft.Data.Sqlite;
using WindUpKey.Protocol;

namespace WindUpRelay.Tests;

/// <summary>Outbox, pair removal and sync-stamp rules of <see cref="RelayStore"/>, against a temporary SQLite file.</summary>
public sealed class RelayStoreRoutingTests : IDisposable
{
    private const string A = "AAAA1111";
    private const string B = "BBBB2222";
    private const string C = "CCCC3333";
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "windup-relay-routing-tests-" + Guid.NewGuid().ToString("N"));
    private readonly string _dbPath;
    private readonly RelayStore _store;

    public RelayStoreRoutingTests()
    {
        _dbPath = Path.Combine(_dir, "relay.db");
        _store = new RelayStore(_dbPath);
    }

    public void Dispose()
    {
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

    private long Enqueue(string from, string to, string type, string? requestId = null) =>
        _store.EnqueueOutbox(requestId, from, to, type, Envelope.Create(type, new { }).Serialize(), TimeSpan.FromDays(1));

    private string? Status(long id) => Scalar("SELECT status FROM outbox WHERE id = $v;", id) as string;

    private object? Scalar(string sql, object value)
    {
        using var conn = new SqliteConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.AddWithValue("$v", value);
        return cmd.ExecuteScalar();
    }

    private void Execute(string sql, params (string Name, object Value)[] args)
    {
        using var conn = new SqliteConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var (name, value) in args)
            cmd.Parameters.AddWithValue(name, value);
        cmd.ExecuteNonQuery();
    }

    private static PairConsentRow Consent(string doll, string peer, DateTimeOffset updated, bool owner = false) =>
        new(doll, peer, false, false, owner, false, updated.ToString("O"));

    [Fact]
    public void RemovePair_PurgesPendingRowsBetweenTheKeys_ButKeepsRemovalNotices()
    {
        Assert.True(_store.TryEstablishPair(A, B));
        var wind = Enqueue(A, B, MessageTypes.Wind, "w");
        var query = Enqueue(B, A, MessageTypes.OwnerSettingsQuery, "q");
        var grant = Enqueue(A, B, MessageTypes.OwnerGrant);
        var established = Enqueue(A, B, MessageTypes.PairEstablished);
        var revoked = Enqueue(A, B, MessageTypes.OwnerRevoked);
        var removed = Enqueue(B, A, MessageTypes.PairRemove);
        var otherPair = Enqueue(A, C, MessageTypes.Wind, "c");
        var delivered = Enqueue(A, B, MessageTypes.Wind, "d");
        _store.MarkDelivered(delivered);

        Assert.Equal(new PairRemoval(Removed: true, Purged: 4), _store.RemovePair(A, B));

        Assert.Null(Status(wind));
        Assert.Null(Status(query));
        Assert.Null(Status(grant));
        Assert.Null(Status(established));
        Assert.Equal("pending", Status(revoked));
        Assert.Equal("pending", Status(removed));
        Assert.Equal("pending", Status(otherPair));
        Assert.Equal("delivered", Status(delivered));
        Assert.False(_store.HasEstablishedPair(A, B));
    }

    [Fact]
    public void RemovePair_SaysWhetherAPairOrAPendingSubmitWasRemoved()
    {
        // 02-D19: the relay queues a pairRemove only when this is true.
        Assert.True(_store.TryEstablishPair(A, B));
        Assert.True(_store.RemovePair(B, A).Removed);
        Assert.False(_store.RemovePair(A, B).Removed);

        // The peer's pending submit toward the remover counts; consent rows and outbox rows alone don't.
        _store.UpsertPairSubmit(C, A);
        Assert.True(_store.RemovePair(A, C).Removed);
        Assert.False(_store.HasPairSubmit(C, A));
        Assert.True(_store.TryMergePairConsent(Consent(A, C, DateTimeOffset.UtcNow)));
        Enqueue(A, C, MessageTypes.Wind, "w");
        Assert.Equal(new PairRemoval(Removed: false, Purged: 1), _store.RemovePair(A, C));
    }

    [Fact]
    public void RemovePair_OfTheRemoversOwnSubmit_RemovesItButDoesNotCount()
    {
        // 02-D25: otherwise submit + remove in a loop would queue a pairRemove to a stranger every time.
        _store.UpsertPairSubmit(A, C);
        Assert.Equal(new PairRemoval(Removed: false, Purged: 0), _store.RemovePair(A, C));
        Assert.False(_store.HasPairSubmit(A, C));

        // Both directions pending: the peer's counts, and both go.
        _store.UpsertPairSubmit(A, C);
        _store.UpsertPairSubmit(C, A);
        Assert.True(_store.RemovePair(A, C).Removed);
        Assert.False(_store.HasPairSubmit(A, C));
        Assert.False(_store.HasPairSubmit(C, A));
    }

    [Fact]
    public void EnqueueOutboxOnce_SkipsWhileTheSameNoticeIsPending()
    {
        string Json() => Envelope.Create(MessageTypes.PairRemove, new { }).Serialize();
        var first = _store.EnqueueOutboxOnce(null, A, B, MessageTypes.PairRemove, Json(), TimeSpan.FromDays(1));
        Assert.NotNull(first);
        Assert.Null(_store.EnqueueOutboxOnce(null, A, B, MessageTypes.PairRemove, Json(), TimeSpan.FromDays(1)));

        // Another sender, recipient or type is its own row.
        Assert.NotNull(_store.EnqueueOutboxOnce(null, C, B, MessageTypes.PairRemove, Json(), TimeSpan.FromDays(1)));
        Assert.NotNull(_store.EnqueueOutboxOnce(null, A, C, MessageTypes.PairRemove, Json(), TimeSpan.FromDays(1)));
        Assert.NotNull(_store.EnqueueOutboxOnce(null, A, B, MessageTypes.OwnerRevoked, Json(), TimeSpan.FromDays(1)));

        // Once delivered (or expired), a new one may be queued.
        _store.MarkDelivered(first!.Value);
        var second = _store.EnqueueOutboxOnce(null, A, B, MessageTypes.PairRemove, Json(), TimeSpan.FromMilliseconds(1));
        Assert.NotNull(second);
        Thread.Sleep(20);
        Assert.NotNull(_store.EnqueueOutboxOnce(null, A, B, MessageTypes.PairRemove, Json(), TimeSpan.FromDays(1)));
        Assert.Equal("expired", Status(second!.Value));
    }

    [Fact]
    public void EnqueueOutboxOnce_DeletesThePendingRowsItSupersedes_FromTheSenderToTheRecipientOnly()
    {
        // 02-D31: an ownerRevoked drops the ownerGrant rows queued before it.
        string Json() => Envelope.Create(MessageTypes.OwnerRevoked, new { }).Serialize();
        var grant = Enqueue(A, B, MessageTypes.OwnerGrant);
        var delivered = Enqueue(A, B, MessageTypes.OwnerGrant);
        _store.MarkDelivered(delivered);
        var reverse = Enqueue(B, A, MessageTypes.OwnerGrant);
        var otherSender = Enqueue(C, B, MessageTypes.OwnerGrant);
        var otherRecipient = Enqueue(A, C, MessageTypes.OwnerGrant);
        var otherType = Enqueue(A, B, MessageTypes.Wind, "w1");

        var revoke = _store.EnqueueOutboxOnce(null, A, B, MessageTypes.OwnerRevoked, Json(), TimeSpan.FromDays(1), MessageTypes.OwnerGrant);
        Assert.NotNull(revoke);
        Assert.Null(Status(grant));
        Assert.Equal("delivered", Status(delivered));
        Assert.Equal("pending", Status(reverse));
        Assert.Equal("pending", Status(otherSender));
        Assert.Equal("pending", Status(otherRecipient));
        Assert.Equal("pending", Status(otherType));

        // A grant queued after it is deleted by the next revoke too, even though that revoke isn't queued again.
        var later = Enqueue(A, B, MessageTypes.OwnerGrant);
        Assert.Null(_store.EnqueueOutboxOnce(null, A, B, MessageTypes.OwnerRevoked, Json(), TimeSpan.FromDays(1), MessageTypes.OwnerGrant));
        Assert.Null(Status(later));
        Assert.Equal("pending", Status(revoke!.Value));
    }

    [Fact]
    public void MarkAckedByRequestId_AcksOnlyRowsToTheAckerFromTheRepliesTo()
    {
        var row = Enqueue(A, B, MessageTypes.Wind, "rid");
        var sameIdElsewhere = Enqueue(C, B, MessageTypes.Wind, "rid");

        _store.MarkAckedByRequestId("rid", ackerKey: C, senderKey: A);
        _store.MarkAckedByRequestId("rid", ackerKey: A, senderKey: B);
        Assert.Equal("pending", Status(row));
        Assert.Equal("pending", Status(sameIdElsewhere));

        _store.MarkAckedByRequestId("rid", ackerKey: B, senderKey: A);
        Assert.Equal("acked", Status(row));
        Assert.Equal("pending", Status(sameIdElsewhere));
    }

    [Fact]
    public void MarkExpired_ExpiresOnlyPendingRows()
    {
        var pending = Enqueue(A, B, MessageTypes.Wind);
        var delivered = Enqueue(A, B, MessageTypes.Wind);
        _store.MarkDelivered(delivered);

        Assert.True(_store.MarkExpired(pending));
        Assert.False(_store.MarkExpired(delivered));
        Assert.Equal("expired", Status(pending));
        Assert.Equal("delivered", Status(delivered));
    }

    [Fact]
    public void DeleteFutureSyncStamps_DeletesOnlyRowsBeyondTheSkew_AndIsIdempotent()
    {
        var now = DateTimeOffset.UtcNow;
        var past = now.AddDays(-1);
        var within = now.AddMinutes(4);
        Assert.True(_store.TryMergePairConsent(Consent(A, B, now.AddYears(50), owner: true)));
        Assert.True(_store.TryMergePairConsent(Consent(A, C, within)));
        Assert.True(_store.TryMergePairConsent(Consent(B, A, past)));
        Assert.True(_store.TryMergeDollWind(A, null, now.AddMinutes(6).ToString("O")));
        Assert.True(_store.TryMergeDollWind(B, null, within.ToString("O")));
        Assert.True(_store.TryMergeDollWind(C, null, past.ToString("O")));

        Assert.Equal(2, _store.DeleteFutureSyncStamps(now));

        // The far-future rows are gone (the doll re-pushes its real state); rows within 5 minutes are untouched.
        var consent = Assert.Single(_store.GetPairConsentForDoll(A));
        Assert.Equal(C, consent.PeerKey);
        Assert.Equal(within.ToString("O"), consent.UpdatedUtc);
        Assert.Equal(past.ToString("O"), Assert.Single(_store.GetPairConsentForDoll(B)).UpdatedUtc);
        Assert.Null(_store.GetDollWind(A));
        Assert.Equal(within.ToString("O"), _store.GetDollWind(B)!.Value.UpdatedUtc);
        Assert.Equal(past.ToString("O"), _store.GetDollWind(C)!.Value.UpdatedUtc);

        // Safe on every start: nothing left to delete, and a later start doesn't touch the rows within the window.
        Assert.Equal(0, _store.DeleteFutureSyncStamps(now));
        Assert.Equal(0, _store.DeleteFutureSyncStamps(now.AddMinutes(1)));
        Assert.Equal(within.ToString("O"), _store.GetDollWind(B)!.Value.UpdatedUtc);

        // The doll's real state is accepted again.
        Assert.True(_store.TryMergePairConsent(Consent(A, B, now)));
        Assert.True(_store.TryMergeDollWind(A, null, now.ToString("O")));
    }

    [Fact]
    public void DeleteFutureSyncStamps_ReadsOlderStoredValues()
    {
        var now = DateTimeOffset.UtcNow;
        Execute(
            "INSERT INTO doll_wind (pairing_key, expiry_utc, updated_utc) VALUES ($k, NULL, $u);",
            ("$k", A),
            ("$u", now.AddDays(3).UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)));

        Assert.Equal(1, _store.DeleteFutureSyncStamps(now));
        Assert.Null(_store.GetDollWind(A));
    }

    [Fact]
    public void Stamps_AreParsedCultureInvariantly()
    {
        var culture = CultureInfo.CurrentCulture;
        try
        {
            // Round-trip values under a non-Gregorian calendar culture.
            CultureInfo.CurrentCulture = new CultureInfo("th-TH");
            var now = DateTimeOffset.UtcNow;
            Assert.True(_store.TryMergePairConsent(Consent(A, B, now)));
            Assert.False(_store.TryMergePairConsent(Consent(A, B, now.AddDays(-1))));
            Assert.True(_store.TryMergePairConsent(Consent(A, B, now.AddSeconds(1))));

            // A stamp far ahead is found by the cleanup, whatever the culture.
            Assert.True(_store.TryMergeDollWind(A, null, now.AddDays(1).ToString("O")));
            Assert.Equal(1, _store.DeleteFutureSyncStamps(now));
        }
        finally
        {
            CultureInfo.CurrentCulture = culture;
        }
    }

    [Fact]
    public void WindRequestCooldown_IsReadCultureInvariantly()
    {
        var culture = CultureInfo.CurrentCulture;
        try
        {
            // Its own round-trip stamps hold under a non-Gregorian calendar culture.
            CultureInfo.CurrentCulture = new CultureInfo("th-TH");
            Assert.True(_store.TryConsumeWindRequest(A, TimeSpan.FromHours(1)));
            Assert.False(_store.TryConsumeWindRequest(A, TimeSpan.FromHours(1)));
            Assert.True(_store.TryConsumeWindRequest(A, TimeSpan.Zero));

            // An older stored value is read month-first (13 January 2000), not day-first as en-GB would try (and fail,
            // which would let the request through).
            CultureInfo.CurrentCulture = new CultureInfo("en-GB");
            Execute(
                "INSERT INTO wind_request_cooldowns (pairing_key, last_request_utc) VALUES ($k, $u);",
                ("$k", B),
                ("$u", "01/13/2000 00:00:00 +00:00"));
            Assert.False(_store.TryConsumeWindRequest(B, TimeSpan.FromDays(100_000)));
            Assert.True(_store.TryConsumeWindRequest(B, TimeSpan.FromDays(1)));
        }
        finally
        {
            CultureInfo.CurrentCulture = culture;
        }
    }

    [Fact]
    public void OlderStoredValues_AreReadMonthFirst_WhateverTheCulture()
    {
        var culture = CultureInfo.CurrentCulture;
        try
        {
            // 12 October (invariant), not 10 December as en-GB would read it.
            CultureInfo.CurrentCulture = new CultureInfo("en-GB");
            Execute(
                "INSERT INTO doll_wind (pairing_key, expiry_utc, updated_utc) VALUES ($k, NULL, $u);",
                ("$k", B),
                ("$u", "10/12/2026 18:30:15 +00:00"));
            Assert.Equal(0, _store.DeleteFutureSyncStamps(new DateTimeOffset(2026, 11, 1, 0, 0, 0, TimeSpan.Zero)));
            Assert.Equal(1, _store.DeleteFutureSyncStamps(new DateTimeOffset(2026, 10, 12, 18, 0, 0, TimeSpan.Zero)));
        }
        finally
        {
            CultureInfo.CurrentCulture = culture;
        }
    }
}
