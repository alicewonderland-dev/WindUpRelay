/// <summary>
/// 05-D2: a per-client-address limit on account creation (trust on first use), so one source can't claim many computed
/// pairing keys, each locking its real owner out until an operator reset. It counts successful creations per client key,
/// found as 03-D9's <see cref="ConnectionGate"/> finds its own (from <c>X-Forwarded-For</c> trusted only from loopback)
/// but grouping IPv6 more widely: an IPv4 address or an IPv6 /48 (<see cref="Ipv6PrefixBits"/>, 05-D7). Over a sliding
/// hour and a sliding day. No sockets or HTTP types, so it can be tested directly. Thread-safe.
/// <para>
/// A slot is taken before the account is inserted and given back (<see cref="Refund"/>) when the insert-if-absent loses a
/// race or throws (05-D11), so only creations count. A null client key (no usable header) is never limited, like the per-IP cap. A limit of
/// 0 turns it off.
/// </para>
/// <para>
/// Memory stays bounded: at most <c>maxKeys</c> client keys are tracked, each with at most the day limit of times (well
/// under 1 KB a key). When the table is full even after dropping keys with nothing left in the day window, a client key
/// that isn't tracked gets no limit (05-D7): the behaviour before 05, and only under attack. They never share one
/// bucket, which would let one source lock every new address out.
/// </para>
/// </summary>
internal sealed class FirstBindLimiter(int perHour, int perDay, TimeProvider? timeProvider = null, int maxKeys = FirstBindLimiter.DefaultMaxKeys)
{
    public const int DefaultPerHour = 5;
    public const int DefaultPerDay = 20;
    public const int DefaultMaxKeys = 64 * 1024;

    /// <summary>05-D7: IPv6 client keys for this limit are /48 networks (a site), not the per-IP cap's /64.</summary>
    public const int Ipv6PrefixBits = 48;

    private static readonly TimeSpan Hour = TimeSpan.FromHours(1);
    private static readonly TimeSpan Day = TimeSpan.FromDays(1);

    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;
    private readonly object _gate = new();
    private readonly Dictionary<string, Queue<DateTimeOffset>> _byClient = new(StringComparer.Ordinal);

    public int PerHour { get; } = Math.Max(0, perHour);

    public int PerDay { get; } = Math.Max(0, perDay);

    /// <summary>False when both limits are 0: nothing is ever counted.</summary>
    public bool Enabled => PerHour > 0 || PerDay > 0;

    public int TrackedKeys
    {
        get
        {
            lock (_gate)
                return _byClient.Count;
        }
    }

    /// <summary>
    /// Takes a creation slot for <paramref name="clientKey"/>. False when its hour or day limit is reached: the register
    /// must create no account. A null key, a limiter with both limits off, or a key that can't be tracked because the
    /// table is full always gets a slot (and counts nothing). <paramref name="lease"/> names the slot to
    /// <see cref="Refund"/> when the creation didn't happen.
    /// </summary>
    public bool TryAcquire(string? clientKey, out FirstBindLease lease)
    {
        lease = default;
        if (clientKey is null || !Enabled)
            return true;

        var now = _time.GetUtcNow();
        lock (_gate)
        {
            if (!CanTrack(clientKey, now))
                return true;

            if (!_byClient.TryGetValue(clientKey, out var times))
                _byClient[clientKey] = times = new Queue<DateTimeOffset>();

            Expire(times, now);
            if (PerDay > 0 && times.Count >= PerDay)
                return false;
            if (PerHour > 0 && CountSince(times, now - Hour) >= PerHour)
                return false;

            times.Enqueue(now);
            lease = new FirstBindLease(clientKey, now);
            return true;
        }
    }

    /// <summary>Gives back a slot whose account wasn't created (a concurrent first bind won the insert, or it threw).</summary>
    public void Refund(FirstBindLease lease)
    {
        if (lease.ClientKey is null)
            return;

        lock (_gate)
        {
            if (!_byClient.TryGetValue(lease.ClientKey, out var times))
                return;

            // Rebuild without the one time this lease took (times are in order; a queue has no remove).
            var kept = new Queue<DateTimeOffset>(times.Count);
            var removed = false;
            foreach (var time in times)
            {
                if (!removed && time == lease.At)
                {
                    removed = true;
                    continue;
                }

                kept.Enqueue(time);
            }

            if (kept.Count == 0)
                _byClient.Remove(lease.ClientKey);
            else
                _byClient[lease.ClientKey] = kept;
        }
    }

    /// <summary>
    /// Whether <paramref name="clientKey"/> is tracked or there's room for it. Prunes keys with nothing left in the day
    /// window only when the table is full.
    /// </summary>
    private bool CanTrack(string clientKey, DateTimeOffset now)
    {
        if (_byClient.ContainsKey(clientKey) || _byClient.Count < maxKeys)
            return true;

        foreach (var (key, times) in _byClient.ToList())
        {
            Expire(times, now);
            if (times.Count == 0)
                _byClient.Remove(key);
        }

        return _byClient.Count < maxKeys;
    }

    private static void Expire(Queue<DateTimeOffset> times, DateTimeOffset now)
    {
        while (times.TryPeek(out var oldest) && now - oldest >= Day)
            times.Dequeue();
    }

    private static int CountSince(Queue<DateTimeOffset> times, DateTimeOffset since)
    {
        var count = 0;
        foreach (var time in times)
        {
            if (time > since)
                count++;
        }

        return count;
    }
}

/// <summary>A slot taken by <see cref="FirstBindLimiter.TryAcquire"/>; the default lease (no key) refunds nothing.</summary>
internal readonly record struct FirstBindLease(string? ClientKey, DateTimeOffset At);
