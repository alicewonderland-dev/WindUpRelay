using Microsoft.Data.Sqlite;
using WindUpKey.Protocol;

namespace WindUpRelay.Tests;

public sealed class RelayStoreAccountTests : IDisposable
{
    private const string Key = "ABCD1234";
    private const string Peer = "WXYZ9876";
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "windup-relay-tests-" + Guid.NewGuid().ToString("N"));
    private readonly string _dbPath;

    public RelayStoreAccountTests()
    {
        _dbPath = Path.Combine(_dir, "relay.db");
    }

    public void Dispose()
    {
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

    private static string NewHash() => RelaySecret.HashHex(RelaySecret.Generate())!;

    [Fact]
    public void UnknownKey_HasNoAccount()
    {
        using var store = new RelayStore(_dbPath);
        Assert.Null(store.GetAccountSecretHash(Key));
    }

    [Fact]
    public void TryCreateAccount_IsInsertIfAbsent()
    {
        using var store = new RelayStore(_dbPath);
        var first = NewHash();
        var second = NewHash();

        Assert.True(store.TryCreateAccount(Key, first));
        Assert.False(store.TryCreateAccount(Key, second));
        Assert.Equal(first, store.GetAccountSecretHash(Key));
        Assert.Null(store.GetAccountSecretHash(Peer));
    }

    [Fact]
    public void TryCreateAccount_SetsTimestamps()
    {
        using var store = new RelayStore(_dbPath);
        store.TryCreateAccount(Key, NewHash());

        var (created, lastSeen) = ReadTimestamps(Key);
        Assert.True(DateTimeOffset.TryParse(created, out _));
        Assert.Equal(created, lastSeen);
    }

    [Fact]
    public void TouchAccount_UpdatesLastSeenOnly()
    {
        using var store = new RelayStore(_dbPath);
        var hash = NewHash();
        store.TryCreateAccount(Key, hash);
        var (created, before) = ReadTimestamps(Key);

        Thread.Sleep(20);
        store.TouchAccount(Key);

        var (createdAfter, after) = ReadTimestamps(Key);
        Assert.Equal(created, createdAfter);
        Assert.True(DateTimeOffset.Parse(after) > DateTimeOffset.Parse(before));
        Assert.Equal(hash, store.GetAccountSecretHash(Key));
    }

    [Fact]
    public void DeleteAccount_LetsTheKeyBeClaimedAgain()
    {
        using var store = new RelayStore(_dbPath);
        store.TryCreateAccount(Key, NewHash());

        Assert.True(store.DeleteAccount(Key));
        Assert.Null(store.GetAccountSecretHash(Key));
        Assert.False(store.DeleteAccount(Key));

        var replacement = NewHash();
        Assert.True(store.TryCreateAccount(Key, replacement));
        Assert.Equal(replacement, store.GetAccountSecretHash(Key));
    }

    [Fact]
    public void Accounts_DoNotTouchExistingKeyedState()
    {
        using var store = new RelayStore(_dbPath);
        store.TryEstablishPair(Key, Peer);
        store.TryMergeDollWind(Key, "2026-10-12T00:00:00.0000000+00:00", "2026-10-11T00:00:00.0000000+00:00");
        store.EnqueueOutbox("r1", Peer, Key, MessageTypes.Wind, "{}", TimeSpan.FromDays(1));

        store.TryCreateAccount(Key, NewHash());
        store.DeleteAccount(Key);

        Assert.Equal([Peer], store.GetPeerKeys(Key));
        Assert.NotNull(store.GetDollWind(Key));
        Assert.Single(store.TakePendingFor(Key));
    }

    [Fact]
    public void ExistingDatabase_UpgradesInPlace()
    {
        // A database created before the accounts table existed, with a durable pair in it.
        Directory.CreateDirectory(_dir);
        using (var conn = new SqliteConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                """
                CREATE TABLE pairs (
                  key_lo TEXT NOT NULL,
                  key_hi TEXT NOT NULL,
                  established_utc TEXT NOT NULL,
                  PRIMARY KEY (key_lo, key_hi),
                  CHECK (key_lo < key_hi)
                );
                INSERT INTO pairs VALUES ('ABCD1234', 'WXYZ9876', '2026-01-01T00:00:00.0000000+00:00');
                """;
            cmd.ExecuteNonQuery();
        }

        SqliteConnection.ClearAllPools();

        using var store = new RelayStore(_dbPath);
        Assert.Equal([Peer], store.GetPeerKeys(Key));
        Assert.Null(store.GetAccountSecretHash(Key));
        Assert.True(store.TryCreateAccount(Key, NewHash()));
    }

    [Fact]
    public void SnapshotStats_CountsAccountsAndRecentClaims()
    {
        using var store = new RelayStore(_dbPath);
        Assert.Equal(0, store.SnapshotStats().Accounts);

        store.TryCreateAccount(Key, NewHash());
        store.TryCreateAccount(Peer, NewHash());
        store.TryCreateAccount("OLDKEY12", NewHash());
        using (var conn = new SqliteConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "UPDATE accounts SET created_utc = $old WHERE pairing_key = 'OLDKEY12';";
            cmd.Parameters.AddWithValue("$old", DateTimeOffset.UtcNow.AddDays(-2).ToString("O"));
            cmd.ExecuteNonQuery();
        }

        var stats = store.SnapshotStats();
        Assert.Equal(3, stats.Accounts);
        Assert.Equal(2, stats.AccountsClaimedLast24h);
    }

    private (string Created, string LastSeen) ReadTimestamps(string key)
    {
        using var conn = new SqliteConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT created_utc, last_seen_utc FROM accounts WHERE pairing_key = $k;";
        cmd.Parameters.AddWithValue("$k", key);
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        return (reader.GetString(0), reader.GetString(1));
    }
}
