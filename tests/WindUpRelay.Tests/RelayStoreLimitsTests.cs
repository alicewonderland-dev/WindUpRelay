using Microsoft.Data.Sqlite;
using WindUpKey.Protocol;

namespace WindUpRelay.Tests;

/// <summary>The 03-D6 outbox limits of <see cref="RelayStore.TryEnqueueOutbox(string?, string, string, string, string, TimeSpan, bool, string?)"/>.</summary>
public sealed class RelayStoreLimitsTests : IDisposable
{
    private const string A = "AAAA1111";
    private const string B = "BBBB2222";
    private const string C = "CCCC3333";
    private static readonly TimeSpan Day = TimeSpan.FromDays(1);
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "windup-relay-limits-tests-" + Guid.NewGuid().ToString("N"));
    private readonly string _dbPath;
    private readonly RelayStore _store;

    public RelayStoreLimitsTests()
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

    private static string Json(string type) => Envelope.Create(type, new { }).Serialize();

    private OutboxEnqueueStatus Try(string from, string to, string type, bool once = false, string? supersedes = null) =>
        _store.TryEnqueueOutbox(null, from, to, type, Json(type), Day, once, supersedes);

    /// <summary>Pending rows inserted as is, many at once (in one transaction: fast).</summary>
    private void Seed(string from, string to, string type, int count)
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
            cmd.Parameters.AddWithValue("$json", Json(type));
            cmd.Parameters.AddWithValue("$now", now.ToString("O"));
            cmd.Parameters.AddWithValue("$exp", now.Add(Day).ToString("O"));
            cmd.ExecuteNonQuery();
        }

        tx.Commit();
    }

    private long Pending(string to, string? type = null)
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

    private void Execute(string sql)
    {
        using var conn = new SqliteConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    [Fact]
    public void ASendersQuota_TakesExactlyOneHundredRows_ThenRefusesTheNewest()
    {
        Seed(A, B, MessageTypes.Wind, WireLimits.OutboxPerSender - 1);

        Assert.Equal(OutboxEnqueueStatus.Queued, Try(A, B, MessageTypes.Wind));
        Assert.Equal(OutboxEnqueueStatus.QuotaFull, Try(A, B, MessageTypes.Unwind));
        Assert.Equal(WireLimits.OutboxPerSender, Pending(B));

        // Another sender to the same recipient, or the same sender to another recipient, still queues.
        Assert.Equal(OutboxEnqueueStatus.Queued, Try(C, B, MessageTypes.Wind));
        Assert.Equal(OutboxEnqueueStatus.Queued, Try(A, C, MessageTypes.Wind));
    }

    [Fact]
    public void ARecipientsQuota_TakesExactlyOneThousandRows_FromAllSendersTogether()
    {
        for (var i = 0; i < 9; i++)
            Seed($"SEND{i:D4}", B, MessageTypes.Wind, 100);
        Seed("SEND0009", B, MessageTypes.Wind, 99);

        Assert.Equal(OutboxEnqueueStatus.Queued, Try(A, B, MessageTypes.Wind));
        Assert.Equal(OutboxEnqueueStatus.QuotaFull, Try(C, B, MessageTypes.Wind));
        Assert.Equal(WireLimits.OutboxPerRecipient, Pending(B));
    }

    [Fact]
    public void RemovalAndPairingNotices_StillQueue_WhenEveryQuotaIsFull()
    {
        for (var i = 0; i < 10; i++)
            Seed($"SEND{i:D4}", B, MessageTypes.Wind, 100);
        Seed(A, B, MessageTypes.OwnerSettingsQuery, 100);
        Assert.Equal(OutboxEnqueueStatus.QuotaFull, Try(A, B, MessageTypes.Wind));

        Assert.Equal(OutboxEnqueueStatus.Queued, Try(A, B, MessageTypes.OwnerRevoked, once: true, supersedes: MessageTypes.OwnerGrant));
        Assert.Equal(OutboxEnqueueStatus.Queued, Try(A, B, MessageTypes.PairRemove, once: true));
        Assert.Equal(OutboxEnqueueStatus.Queued, Try(A, B, MessageTypes.PairEstablished));
        Assert.Equal(1, Pending(B, MessageTypes.OwnerRevoked));
        Assert.Equal(1, Pending(B, MessageTypes.PairRemove));
        Assert.Equal(1, Pending(B, MessageTypes.PairEstablished));

        // They keep their one-copy rules.
        Assert.Equal(OutboxEnqueueStatus.AlreadyPending, Try(A, B, MessageTypes.OwnerRevoked, once: true));
        Assert.Equal(OutboxEnqueueStatus.AlreadyPending, Try(A, B, MessageTypes.PairRemove, once: true));
    }

    [Fact]
    public void ExemptRows_AndRowsNoLongerPending_DoNotCountTowardAQuota()
    {
        Seed(A, B, MessageTypes.PairRemove, 50);
        Seed(A, B, MessageTypes.OwnerRevoked, 50);
        Seed(A, B, MessageTypes.PairEstablished, 50);
        Seed(A, B, MessageTypes.Wind, 50);
        Execute("UPDATE outbox SET status = 'delivered' WHERE msg_type = 'wind';");
        Seed(A, B, MessageTypes.Wind, 50);
        Execute("UPDATE outbox SET status = 'expired' WHERE msg_type = 'wind' AND status = 'pending';");
        Seed(A, B, MessageTypes.Wind, WireLimits.OutboxPerSender - 1);

        Assert.Equal(OutboxEnqueueStatus.Queued, Try(A, B, MessageTypes.Wind));
        Assert.Equal(OutboxEnqueueStatus.QuotaFull, Try(A, B, MessageTypes.Wind));
    }

    [Fact]
    public void ARow_UpToFourKiB_IsQueued_OneByteMoreIsNot()
    {
        var prefix = "{\"type\":\"wind\",\"payload\":{},\"pad\":\"";
        string Row(int bytes) => prefix + new string('x', bytes - prefix.Length - 2) + "\"}";
        Assert.Equal(WireLimits.OutboxRowBytes, System.Text.Encoding.UTF8.GetByteCount(Row(WireLimits.OutboxRowBytes)));

        Assert.Equal(OutboxEnqueueStatus.Queued, _store.TryEnqueueOutbox(null, A, B, MessageTypes.Wind, Row(WireLimits.OutboxRowBytes), Day));
        Assert.Equal(OutboxEnqueueStatus.RowTooBig, _store.TryEnqueueOutbox(null, A, B, MessageTypes.Wind, Row(WireLimits.OutboxRowBytes + 1), Day));
        Assert.Equal(1, Pending(B));
    }

    [Fact]
    public void Grants_AreQueuedOncePerDollAndOwner_AndARevokeStillSupersedesThem()
    {
        Assert.Equal(OutboxEnqueueStatus.Queued, Try(A, B, MessageTypes.OwnerGrant, once: true));
        Assert.Equal(OutboxEnqueueStatus.AlreadyPending, Try(A, B, MessageTypes.OwnerGrant, once: true));
        Assert.Equal(OutboxEnqueueStatus.Queued, Try(C, B, MessageTypes.OwnerGrant, once: true));
        Assert.Equal(2, Pending(B, MessageTypes.OwnerGrant));

        // 02-D31: the revoke deletes A's pending grant; a grant after it queues again, and the next revoke deletes it.
        Assert.Equal(OutboxEnqueueStatus.Queued, Try(A, B, MessageTypes.OwnerRevoked, once: true, supersedes: MessageTypes.OwnerGrant));
        Assert.Equal(1, Pending(B, MessageTypes.OwnerGrant));
        Assert.Equal(OutboxEnqueueStatus.Queued, Try(A, B, MessageTypes.OwnerGrant, once: true));
        Assert.Equal(OutboxEnqueueStatus.AlreadyPending, Try(A, B, MessageTypes.OwnerRevoked, once: true, supersedes: MessageTypes.OwnerGrant));
        Assert.Equal(1, Pending(B, MessageTypes.OwnerGrant));
    }

    [Fact]
    public void Grants_CountTowardTheQuotas()
    {
        Seed(A, B, MessageTypes.Wind, WireLimits.OutboxPerSender);

        Assert.Equal(OutboxEnqueueStatus.QuotaFull, Try(A, B, MessageTypes.OwnerGrant, once: true));
        Assert.Equal(0, Pending(B, MessageTypes.OwnerGrant));
    }

    // ---- 03-D18: a wind request refused with queue_full gives its cooldown back ----

    [Fact]
    public void ARefundedWindRequest_LeavesTheCooldownAsItWas()
    {
        var hour = TimeSpan.FromHours(1);

        // No earlier request: the refund removes the stamp.
        Assert.True(_store.TryConsumeWindRequest(A, hour, out var first));
        _store.RefundWindRequest(first);
        Assert.True(_store.TryConsumeWindRequest(A, hour, out _));
        Assert.False(_store.TryConsumeWindRequest(A, hour));

        // An earlier stamp: the refund restores it rather than deleting it, so the earlier request's cooldown still holds.
        Assert.True(_store.TryConsumeWindRequest(B, TimeSpan.Zero));
        Assert.True(_store.TryConsumeWindRequest(B, TimeSpan.Zero, out var second));
        _store.RefundWindRequest(second);
        Assert.False(_store.TryConsumeWindRequest(B, hour));

        // A default (nothing consumed) refunds nothing.
        _store.RefundWindRequest(default);
    }

    [Fact]
    public void ARefund_NeverUndoesALaterRequestsCooldown()
    {
        var hour = TimeSpan.FromHours(1);
        Assert.True(_store.TryConsumeWindRequest(A, hour, out var refused));
        Assert.True(_store.TryConsumeWindRequest(A, TimeSpan.Zero)); // a later request, accepted meanwhile

        _store.RefundWindRequest(refused);

        Assert.False(_store.TryConsumeWindRequest(A, hour));
    }
}
