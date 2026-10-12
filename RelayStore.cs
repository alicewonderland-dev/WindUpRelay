using Microsoft.Data.Sqlite;

/// <summary>
/// SQLite persistence for durable pairs, doll consent, wind expiry, pending submits,
/// offline outbox, rate limits, relay accounts, and ops events.
/// Never stores Name@World, ContentId, relay tokens, or raw relay secrets (accounts keep only SHA-256 hex).
/// </summary>
internal sealed class RelayStore : IDisposable
{
    private readonly string _connectionString;
    private readonly object _gate = new();

    public RelayStore(string databasePath)
    {
        var dir = Path.GetDirectoryName(databasePath);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);

        _connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = databasePath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Shared,
        }.ToString();

        Initialize();
    }

    public void Dispose()
    {
        // Connections are opened per-call.
    }

    private SqliteConnection Open()
    {
        var conn = new SqliteConnection(_connectionString);
        conn.Open();
        using var pragma = conn.CreateCommand();
        pragma.CommandText = "PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;";
        pragma.ExecuteNonQuery();
        return conn;
    }

    private void Initialize()
    {
        lock (_gate)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                """
                CREATE TABLE IF NOT EXISTS pair_submits (
                  my_key TEXT NOT NULL,
                  their_key TEXT NOT NULL,
                  created_utc TEXT NOT NULL,
                  PRIMARY KEY (my_key, their_key)
                );

                CREATE TABLE IF NOT EXISTS pairs (
                  key_lo TEXT NOT NULL,
                  key_hi TEXT NOT NULL,
                  established_utc TEXT NOT NULL,
                  PRIMARY KEY (key_lo, key_hi),
                  CHECK (key_lo < key_hi)
                );

                CREATE TABLE IF NOT EXISTS outbox (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  request_id TEXT,
                  to_key TEXT NOT NULL,
                  from_key TEXT NOT NULL,
                  msg_type TEXT NOT NULL,
                  envelope_json TEXT NOT NULL,
                  created_utc TEXT NOT NULL,
                  delivered_utc TEXT,
                  acked_utc TEXT,
                  expires_utc TEXT NOT NULL,
                  status TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS ix_outbox_pending_to
                  ON outbox(to_key, status, id);

                CREATE INDEX IF NOT EXISTS ix_outbox_request
                  ON outbox(request_id);

                CREATE TABLE IF NOT EXISTS rate_buckets (
                  pairing_key TEXT NOT NULL,
                  window_start TEXT NOT NULL,
                  count INTEGER NOT NULL,
                  PRIMARY KEY (pairing_key, window_start)
                );

                CREATE TABLE IF NOT EXISTS wind_request_cooldowns (
                  pairing_key TEXT PRIMARY KEY,
                  last_request_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS relay_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  utc TEXT NOT NULL,
                  kind TEXT NOT NULL,
                  pairing_key TEXT,
                  peer_key TEXT,
                  msg_type TEXT,
                  detail TEXT
                );

                CREATE INDEX IF NOT EXISTS ix_relay_events_utc
                  ON relay_events(utc);

                CREATE TABLE IF NOT EXISTS doll_wind (
                  pairing_key TEXT PRIMARY KEY,
                  expiry_utc TEXT,
                  updated_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS pair_consent (
                  doll_key TEXT NOT NULL,
                  peer_key TEXT NOT NULL,
                  can_wind INTEGER NOT NULL,
                  can_unwind INTEGER NOT NULL,
                  is_owner INTEGER NOT NULL,
                  can_call INTEGER NOT NULL,
                  updated_utc TEXT NOT NULL,
                  PRIMARY KEY (doll_key, peer_key)
                );

                CREATE TABLE IF NOT EXISTS accounts (
                  pairing_key TEXT PRIMARY KEY,
                  secret_hash TEXT NOT NULL,
                  created_utc TEXT NOT NULL,
                  last_seen_utc TEXT NOT NULL
                );
                """;
            cmd.ExecuteNonQuery();
        }
    }

    /// <summary>Secret hash (hex) of the account for this key, or null when the key is unclaimed.</summary>
    public string? GetAccountSecretHash(string pairingKey)
    {
        lock (_gate)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT secret_hash FROM accounts WHERE pairing_key = $k;";
            cmd.Parameters.AddWithValue("$k", pairingKey);
            return cmd.ExecuteScalar() as string;
        }
    }

    /// <summary>
    /// Insert-if-absent. Returns false when the key already has an account (e.g. a concurrent first bind won);
    /// callers re-read with <see cref="GetAccountSecretHash"/> and decide again.
    /// </summary>
    public bool TryCreateAccount(string pairingKey, string secretHash)
    {
        lock (_gate)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                """
                INSERT INTO accounts (pairing_key, secret_hash, created_utc, last_seen_utc)
                VALUES ($k, $h, $utc, $utc)
                ON CONFLICT(pairing_key) DO NOTHING;
                """;
            cmd.Parameters.AddWithValue("$k", pairingKey);
            cmd.Parameters.AddWithValue("$h", secretHash);
            cmd.Parameters.AddWithValue("$utc", UtcNow());
            return cmd.ExecuteNonQuery() > 0;
        }
    }

    public void TouchAccount(string pairingKey)
    {
        lock (_gate)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "UPDATE accounts SET last_seen_utc = $utc WHERE pairing_key = $k;";
            cmd.Parameters.AddWithValue("$k", pairingKey);
            cmd.Parameters.AddWithValue("$utc", UtcNow());
            cmd.ExecuteNonQuery();
        }
    }

    /// <summary>Admin reset: the next authenticated register claims the key again. Returns false when absent.</summary>
    public bool DeleteAccount(string pairingKey)
    {
        lock (_gate)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "DELETE FROM accounts WHERE pairing_key = $k;";
            cmd.Parameters.AddWithValue("$k", pairingKey);
            return cmd.ExecuteNonQuery() > 0;
        }
    }

    public void UpsertPairSubmit(string myKey, string theirKey)
    {
        lock (_gate)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                """
                INSERT INTO pair_submits (my_key, their_key, created_utc)
                VALUES ($my, $their, $utc)
                ON CONFLICT(my_key, their_key) DO UPDATE SET created_utc = excluded.created_utc;
                """;
            cmd.Parameters.AddWithValue("$my", myKey);
            cmd.Parameters.AddWithValue("$their", theirKey);
            cmd.Parameters.AddWithValue("$utc", UtcNow());
            cmd.ExecuteNonQuery();
        }
    }

    public bool HasPairSubmit(string myKey, string theirKey)
    {
        lock (_gate)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                "SELECT 1 FROM pair_submits WHERE my_key = $my AND their_key = $their LIMIT 1;";
            cmd.Parameters.AddWithValue("$my", myKey);
            cmd.Parameters.AddWithValue("$their", theirKey);
            return cmd.ExecuteScalar() is not null;
        }
    }

    public void CancelPairSubmit(string myKey, string theirKey)
    {
        lock (_gate)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                "DELETE FROM pair_submits WHERE my_key = $my AND their_key = $their;";
            cmd.Parameters.AddWithValue("$my", myKey);
            cmd.Parameters.AddWithValue("$their", theirKey);
            cmd.ExecuteNonQuery();
        }
    }

    public bool HasEstablishedPair(string a, string b)
    {
        Canonical(a, b, out var lo, out var hi);
        lock (_gate)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                "SELECT 1 FROM pairs WHERE key_lo = $lo AND key_hi = $hi LIMIT 1;";
            cmd.Parameters.AddWithValue("$lo", lo);
            cmd.Parameters.AddWithValue("$hi", hi);
            return cmd.ExecuteScalar() is not null;
        }
    }

    public void ClearPairSubmits(string a, string b)
    {
        lock (_gate)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                """
                DELETE FROM pair_submits
                WHERE (my_key = $a AND their_key = $b)
                   OR (my_key = $b AND their_key = $a);
                """;
            cmd.Parameters.AddWithValue("$a", a);
            cmd.Parameters.AddWithValue("$b", b);
            cmd.ExecuteNonQuery();
        }
    }

    public bool TryEstablishPair(string a, string b)
    {
        Canonical(a, b, out var lo, out var hi);
        lock (_gate)
        {
            using var conn = Open();
            using var tx = conn.BeginTransaction();
            using (var clear = conn.CreateCommand())
            {
                clear.Transaction = tx;
                clear.CommandText =
                    """
                    DELETE FROM pair_submits
                    WHERE (my_key = $a AND their_key = $b)
                       OR (my_key = $b AND their_key = $a);
                    """;
                clear.Parameters.AddWithValue("$a", a);
                clear.Parameters.AddWithValue("$b", b);
                clear.ExecuteNonQuery();
            }

            using (var insert = conn.CreateCommand())
            {
                insert.Transaction = tx;
                insert.CommandText =
                    """
                    INSERT INTO pairs (key_lo, key_hi, established_utc)
                    VALUES ($lo, $hi, $utc)
                    ON CONFLICT(key_lo, key_hi) DO NOTHING;
                    """;
                insert.Parameters.AddWithValue("$lo", lo);
                insert.Parameters.AddWithValue("$hi", hi);
                insert.Parameters.AddWithValue("$utc", UtcNow());
                var added = insert.ExecuteNonQuery() > 0;
                tx.Commit();
                return added;
            }
        }
    }

    public void RemovePair(string a, string b)
    {
        Canonical(a, b, out var lo, out var hi);
        lock (_gate)
        {
            using var conn = Open();
            using var tx = conn.BeginTransaction();
            using (var del = conn.CreateCommand())
            {
                del.Transaction = tx;
                del.CommandText = "DELETE FROM pairs WHERE key_lo = $lo AND key_hi = $hi;";
                del.Parameters.AddWithValue("$lo", lo);
                del.Parameters.AddWithValue("$hi", hi);
                del.ExecuteNonQuery();
            }

            using (var clear = conn.CreateCommand())
            {
                clear.Transaction = tx;
                clear.CommandText =
                    """
                    DELETE FROM pair_submits
                    WHERE (my_key = $a AND their_key = $b)
                       OR (my_key = $b AND their_key = $a);
                    """;
                clear.Parameters.AddWithValue("$a", a);
                clear.Parameters.AddWithValue("$b", b);
                clear.ExecuteNonQuery();
            }

            using (var consent = conn.CreateCommand())
            {
                consent.Transaction = tx;
                consent.CommandText =
                    """
                    DELETE FROM pair_consent
                    WHERE (doll_key = $a AND peer_key = $b)
                       OR (doll_key = $b AND peer_key = $a);
                    """;
                consent.Parameters.AddWithValue("$a", a);
                consent.Parameters.AddWithValue("$b", b);
                consent.ExecuteNonQuery();
            }

            tx.Commit();
        }
    }

    /// <summary>Last-write-wins merge of doll wind expiry. Returns true when the store changed.</summary>
    public bool TryMergeDollWind(string pairingKey, string? expiryUtc, string updatedUtc)
    {
        if (!TryParseUtc(updatedUtc, out var inboundUpdated))
            return false;

        lock (_gate)
        {
            using var conn = Open();
            using var tx = conn.BeginTransaction();
            DateTimeOffset? storedUpdated = null;
            using (var sel = conn.CreateCommand())
            {
                sel.Transaction = tx;
                sel.CommandText = "SELECT updated_utc FROM doll_wind WHERE pairing_key = $k;";
                sel.Parameters.AddWithValue("$k", pairingKey);
                if (sel.ExecuteScalar() is string raw && TryParseUtc(raw, out var parsed))
                    storedUpdated = parsed;
            }

            if (storedUpdated is { } existing && inboundUpdated <= existing)
            {
                tx.Commit();
                return false;
            }

            using (var upsert = conn.CreateCommand())
            {
                upsert.Transaction = tx;
                upsert.CommandText =
                    """
                    INSERT INTO doll_wind (pairing_key, expiry_utc, updated_utc)
                    VALUES ($k, $exp, $upd)
                    ON CONFLICT(pairing_key) DO UPDATE SET
                      expiry_utc = excluded.expiry_utc,
                      updated_utc = excluded.updated_utc;
                    """;
                upsert.Parameters.AddWithValue("$k", pairingKey);
                upsert.Parameters.AddWithValue("$exp", (object?)expiryUtc ?? DBNull.Value);
                upsert.Parameters.AddWithValue("$upd", inboundUpdated.ToString("O"));
                upsert.ExecuteNonQuery();
            }

            tx.Commit();
            return true;
        }
    }

    public DollWindRow? GetDollWind(string pairingKey)
    {
        lock (_gate)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                "SELECT expiry_utc, updated_utc FROM doll_wind WHERE pairing_key = $k;";
            cmd.Parameters.AddWithValue("$k", pairingKey);
            using var reader = cmd.ExecuteReader();
            if (!reader.Read())
                return null;

            return new DollWindRow(
                pairingKey,
                reader.IsDBNull(0) ? null : reader.GetString(0),
                reader.GetString(1));
        }
    }

    /// <summary>Last-write-wins merge of directed doll consent toward a peer.</summary>
    public bool TryMergePairConsent(PairConsentRow inbound)
    {
        if (!TryParseUtc(inbound.UpdatedUtc, out var inboundUpdated))
            return false;

        lock (_gate)
        {
            using var conn = Open();
            using var tx = conn.BeginTransaction();
            DateTimeOffset? storedUpdated = null;
            using (var sel = conn.CreateCommand())
            {
                sel.Transaction = tx;
                sel.CommandText =
                    "SELECT updated_utc FROM pair_consent WHERE doll_key = $d AND peer_key = $p;";
                sel.Parameters.AddWithValue("$d", inbound.DollKey);
                sel.Parameters.AddWithValue("$p", inbound.PeerKey);
                if (sel.ExecuteScalar() is string raw && TryParseUtc(raw, out var parsed))
                    storedUpdated = parsed;
            }

            if (storedUpdated is { } existing && inboundUpdated <= existing)
            {
                tx.Commit();
                return false;
            }

            using (var upsert = conn.CreateCommand())
            {
                upsert.Transaction = tx;
                upsert.CommandText =
                    """
                    INSERT INTO pair_consent
                      (doll_key, peer_key, can_wind, can_unwind, is_owner, can_call, updated_utc)
                    VALUES
                      ($d, $p, $w, $u, $o, $c, $upd)
                    ON CONFLICT(doll_key, peer_key) DO UPDATE SET
                      can_wind = excluded.can_wind,
                      can_unwind = excluded.can_unwind,
                      is_owner = excluded.is_owner,
                      can_call = excluded.can_call,
                      updated_utc = excluded.updated_utc;
                    """;
                upsert.Parameters.AddWithValue("$d", inbound.DollKey);
                upsert.Parameters.AddWithValue("$p", inbound.PeerKey);
                upsert.Parameters.AddWithValue("$w", inbound.CanWind ? 1 : 0);
                upsert.Parameters.AddWithValue("$u", inbound.CanUnwind ? 1 : 0);
                upsert.Parameters.AddWithValue("$o", inbound.IsOwner ? 1 : 0);
                upsert.Parameters.AddWithValue("$c", inbound.CanCall ? 1 : 0);
                upsert.Parameters.AddWithValue("$upd", inboundUpdated.ToString("O"));
                upsert.ExecuteNonQuery();
            }

            tx.Commit();
            return true;
        }
    }

    public IReadOnlyList<PairConsentRow> GetPairConsentForDoll(string dollKey)
    {
        lock (_gate)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                """
                SELECT peer_key, can_wind, can_unwind, is_owner, can_call, updated_utc
                FROM pair_consent
                WHERE doll_key = $d
                ORDER BY peer_key;
                """;
            cmd.Parameters.AddWithValue("$d", dollKey);
            var list = new List<PairConsentRow>();
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                list.Add(new PairConsentRow(
                    dollKey,
                    reader.GetString(0),
                    reader.GetInt64(1) != 0,
                    reader.GetInt64(2) != 0,
                    reader.GetInt64(3) != 0,
                    reader.GetInt64(4) != 0,
                    reader.GetString(5)));
            }

            return list;
        }
    }

    public IReadOnlyList<string> GetPeerKeys(string pairingKey)
    {
        lock (_gate)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                """
                SELECT CASE WHEN key_lo = $k THEN key_hi ELSE key_lo END AS peer
                FROM pairs
                WHERE key_lo = $k OR key_hi = $k
                ORDER BY peer;
                """;
            cmd.Parameters.AddWithValue("$k", pairingKey);
            var list = new List<string>();
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                list.Add(reader.GetString(0));
            return list;
        }
    }

    public IReadOnlyList<(string MyKey, string TheirKey)> GetMutualPendingFor(string pairingKey)
    {
        lock (_gate)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                """
                SELECT s.their_key
                FROM pair_submits s
                INNER JOIN pair_submits r
                  ON r.my_key = s.their_key AND r.their_key = s.my_key
                WHERE s.my_key = $k;
                """;
            cmd.Parameters.AddWithValue("$k", pairingKey);
            var list = new List<(string, string)>();
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var their = reader.GetString(0);
                list.Add((pairingKey, their));
            }

            return list;
        }
    }

    public long EnqueueOutbox(
        string? requestId,
        string fromKey,
        string toKey,
        string msgType,
        string envelopeJson,
        TimeSpan ttl)
    {
        lock (_gate)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                """
                INSERT INTO outbox
                  (request_id, to_key, from_key, msg_type, envelope_json, created_utc, expires_utc, status)
                VALUES
                  ($rid, $to, $from, $type, $json, $now, $exp, 'pending');
                """;
            cmd.Parameters.AddWithValue("$rid", (object?)requestId ?? DBNull.Value);
            cmd.Parameters.AddWithValue("$to", toKey);
            cmd.Parameters.AddWithValue("$from", fromKey);
            cmd.Parameters.AddWithValue("$type", msgType);
            cmd.Parameters.AddWithValue("$json", envelopeJson);
            var now = DateTimeOffset.UtcNow;
            cmd.Parameters.AddWithValue("$now", now.ToString("O"));
            cmd.Parameters.AddWithValue("$exp", now.Add(ttl).ToString("O"));
            cmd.ExecuteNonQuery();

            using var idCmd = conn.CreateCommand();
            idCmd.CommandText = "SELECT last_insert_rowid();";
            return (long)(idCmd.ExecuteScalar() ?? 0L);
        }
    }

    public IReadOnlyList<OutboxRow> TakePendingFor(string toKey, int limit = 50)
    {
        lock (_gate)
        {
            ExpireLocked();
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                """
                SELECT id, request_id, from_key, msg_type, envelope_json
                FROM outbox
                WHERE to_key = $to AND status = 'pending'
                ORDER BY id
                LIMIT $limit;
                """;
            cmd.Parameters.AddWithValue("$to", toKey);
            cmd.Parameters.AddWithValue("$limit", limit);
            var list = new List<OutboxRow>();
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                list.Add(new OutboxRow(
                    reader.GetInt64(0),
                    reader.IsDBNull(1) ? null : reader.GetString(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    reader.GetString(4)));
            }

            return list;
        }
    }

    public void MarkDelivered(long id)
    {
        lock (_gate)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                """
                UPDATE outbox
                SET status = 'delivered', delivered_utc = $utc
                WHERE id = $id AND status = 'pending';
                """;
            cmd.Parameters.AddWithValue("$id", id);
            cmd.Parameters.AddWithValue("$utc", UtcNow());
            cmd.ExecuteNonQuery();
        }
    }

    public void MarkAckedByRequestId(string requestId)
    {
        if (string.IsNullOrWhiteSpace(requestId))
            return;

        lock (_gate)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                """
                UPDATE outbox
                SET status = 'acked', acked_utc = $utc
                WHERE request_id = $rid AND status IN ('pending', 'delivered');
                """;
            cmd.Parameters.AddWithValue("$rid", requestId);
            cmd.Parameters.AddWithValue("$utc", UtcNow());
            cmd.ExecuteNonQuery();
        }
    }

    public int ExpirePending()
    {
        lock (_gate)
            return ExpireLocked();
    }

    private int ExpireLocked()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText =
            """
            UPDATE outbox
            SET status = 'expired'
            WHERE status = 'pending' AND expires_utc < $now;
            """;
        cmd.Parameters.AddWithValue("$now", UtcNow());
        return cmd.ExecuteNonQuery();
    }

    /// <summary>Returns false when the key exceeds <paramref name="maxPerWindow"/> in the current minute window.</summary>
    public bool TryConsumeRate(string pairingKey, int maxPerWindow)
    {
        var window = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm");
        lock (_gate)
        {
            using var conn = Open();
            using var tx = conn.BeginTransaction();
            int count;
            using (var sel = conn.CreateCommand())
            {
                sel.Transaction = tx;
                sel.CommandText =
                    """
                    SELECT count FROM rate_buckets
                    WHERE pairing_key = $k AND window_start = $w;
                    """;
                sel.Parameters.AddWithValue("$k", pairingKey);
                sel.Parameters.AddWithValue("$w", window);
                var scalar = sel.ExecuteScalar();
                count = scalar is long l ? (int)l : scalar is int i ? i : 0;
            }

            if (count >= maxPerWindow)
            {
                tx.Commit();
                return false;
            }

            using (var upsert = conn.CreateCommand())
            {
                upsert.Transaction = tx;
                upsert.CommandText =
                    """
                    INSERT INTO rate_buckets (pairing_key, window_start, count)
                    VALUES ($k, $w, 1)
                    ON CONFLICT(pairing_key, window_start)
                    DO UPDATE SET count = count + 1;
                    """;
                upsert.Parameters.AddWithValue("$k", pairingKey);
                upsert.Parameters.AddWithValue("$w", window);
                upsert.ExecuteNonQuery();
            }

            // Opportunistic prune of old windows.
            using (var prune = conn.CreateCommand())
            {
                prune.Transaction = tx;
                prune.CommandText = "DELETE FROM rate_buckets WHERE window_start < $cut;";
                prune.Parameters.AddWithValue("$cut", DateTimeOffset.UtcNow.AddMinutes(-10).ToString("yyyy-MM-ddTHH:mm"));
                prune.ExecuteNonQuery();
            }

            tx.Commit();
            return true;
        }
    }

    /// <summary>Atomically permits at most one wind request per pairing key per cooldown period.</summary>
    public bool TryConsumeWindRequest(string pairingKey, TimeSpan cooldown)
    {
        var now = DateTimeOffset.UtcNow;
        lock (_gate)
        {
            using var conn = Open();
            using var tx = conn.BeginTransaction();
            DateTimeOffset? lastRequest = null;
            using (var select = conn.CreateCommand())
            {
                select.Transaction = tx;
                select.CommandText =
                    "SELECT last_request_utc FROM wind_request_cooldowns WHERE pairing_key = $key;";
                select.Parameters.AddWithValue("$key", pairingKey);
                if (select.ExecuteScalar() is string raw
                    && DateTimeOffset.TryParse(raw, out var parsed))
                {
                    lastRequest = parsed;
                }
            }

            if (lastRequest is { } last && now - last < cooldown)
            {
                tx.Commit();
                return false;
            }

            using (var upsert = conn.CreateCommand())
            {
                upsert.Transaction = tx;
                upsert.CommandText =
                    """
                    INSERT INTO wind_request_cooldowns (pairing_key, last_request_utc)
                    VALUES ($key, $utc)
                    ON CONFLICT(pairing_key)
                    DO UPDATE SET last_request_utc = excluded.last_request_utc;
                    """;
                upsert.Parameters.AddWithValue("$key", pairingKey);
                upsert.Parameters.AddWithValue("$utc", now.ToString("O"));
                upsert.ExecuteNonQuery();
            }

            tx.Commit();
            return true;
        }
    }

    public void RecordEvent(string kind, string? pairingKey = null, string? peerKey = null, string? msgType = null, string? detail = null)
    {
        lock (_gate)
        {
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                """
                INSERT INTO relay_events (utc, kind, pairing_key, peer_key, msg_type, detail)
                VALUES ($utc, $kind, $pk, $peer, $type, $detail);
                """;
            cmd.Parameters.AddWithValue("$utc", UtcNow());
            cmd.Parameters.AddWithValue("$kind", kind);
            cmd.Parameters.AddWithValue("$pk", (object?)pairingKey ?? DBNull.Value);
            cmd.Parameters.AddWithValue("$peer", (object?)peerKey ?? DBNull.Value);
            cmd.Parameters.AddWithValue("$type", (object?)msgType ?? DBNull.Value);
            cmd.Parameters.AddWithValue("$detail", (object?)detail ?? DBNull.Value);
            cmd.ExecuteNonQuery();

            // Cap table growth.
            using var trim = conn.CreateCommand();
            trim.CommandText =
                """
                DELETE FROM relay_events
                WHERE id < (SELECT COALESCE(MAX(id), 0) - 5000 FROM relay_events);
                """;
            trim.ExecuteNonQuery();
        }
    }

    public RelayStats SnapshotStats()
    {
        lock (_gate)
        {
            ExpireLocked();
            using var conn = Open();
            var cut = DateTimeOffset.UtcNow.AddHours(-24).ToString("O");
            return new RelayStats(
                Count(conn, "SELECT COUNT(*) FROM pairs"),
                Count(conn, "SELECT COUNT(*) FROM pair_submits"),
                Count(conn, "SELECT COUNT(*) FROM outbox WHERE status = 'pending'"),
                Count(conn, "SELECT COUNT(*) FROM outbox WHERE status = 'delivered'"),
                Count(conn, "SELECT COUNT(*) FROM outbox WHERE status = 'acked'"),
                Count(conn, "SELECT COUNT(*) FROM outbox WHERE status = 'expired'"),
                Count(conn, "SELECT COUNT(*) FROM relay_events WHERE utc >= $cut", ("$cut", cut)),
                Count(conn, "SELECT COUNT(*) FROM accounts"),
                Count(conn, "SELECT COUNT(*) FROM accounts WHERE created_utc >= $cut", ("$cut", cut)));
        }
    }

    private static long Count(SqliteConnection conn, string sql, params (string Name, object Value)[] args)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var (name, value) in args)
            cmd.Parameters.AddWithValue(name, value);
        var scalar = cmd.ExecuteScalar();
        return scalar is long l ? l : Convert.ToInt64(scalar ?? 0L);
    }

    private static void Canonical(string a, string b, out string lo, out string hi)
    {
        if (string.CompareOrdinal(a, b) <= 0)
        {
            lo = a;
            hi = b;
        }
        else
        {
            lo = b;
            hi = a;
        }
    }

    private static string UtcNow() => DateTimeOffset.UtcNow.ToString("O");

    private static bool TryParseUtc(string raw, out DateTimeOffset value)
        => DateTimeOffset.TryParse(raw, out value);
}

internal readonly record struct OutboxRow(
    long Id,
    string? RequestId,
    string FromKey,
    string MsgType,
    string EnvelopeJson);

internal readonly record struct DollWindRow(
    string PairingKey,
    string? ExpiryUtc,
    string UpdatedUtc);

internal readonly record struct PairConsentRow(
    string DollKey,
    string PeerKey,
    bool CanWind,
    bool CanUnwind,
    bool IsOwner,
    bool CanCall,
    string UpdatedUtc);

internal readonly record struct RelayStats(
    long Pairs,
    long PendingSubmits,
    long OutboxPending,
    long OutboxDelivered,
    long OutboxAcked,
    long OutboxExpired,
    long EventsLast24h,
    long Accounts,
    long AccountsClaimedLast24h);
