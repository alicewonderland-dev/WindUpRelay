using System.Collections.Concurrent;
using WindUpKey.Protocol;

/// <summary>Session-map rules for register, kept free of sockets so they can be tested directly.</summary>
internal static class SessionMap
{
    /// <summary>
    /// Atomically points <paramref name="key"/> at <paramref name="session"/>. An open holder blocks a legacy
    /// (unauthenticated) claim, which keeps today's pair_key_collision; an authenticated claim always wins.
    /// <paramref name="displaced"/> is the open session that was actually swapped out, which the caller must
    /// strip and close. The map already points at <paramref name="session"/> when this returns true.
    /// </summary>
    public static bool TryClaim<TSession>(
        ConcurrentDictionary<string, TSession> map,
        string key,
        TSession session,
        bool authenticated,
        Func<TSession, bool> isOpen,
        out TSession? displaced)
        where TSession : class
    {
        displaced = null;
        while (true)
        {
            if (map.TryGetValue(key, out var existing))
            {
                if (ReferenceEquals(existing, session))
                    return true;

                var existingOpen = isOpen(existing);
                if (existingOpen && !authenticated)
                    return false;

                if (map.TryUpdate(key, session, existing))
                {
                    displaced = existingOpen ? existing : null;
                    return true;
                }
            }
            else if (map.TryAdd(key, session))
            {
                return true;
            }

            // Lost a race with another register for this key: re-read and decide again.
        }
    }
}

/// <summary>
/// In-memory limiter for repetitive per-key records. Old clients retry a refused register every second or two
/// forever, so <c>register_refused</c> is recorded and logged at most once per key per window; the refusal itself
/// still happens every time.
/// <para>
/// Memory and CPU stay bounded under key churn: expired keys are pruned at most once per window (one scan), and at
/// most <c>maxKeys</c> keys are tracked. When full, keys that aren't tracked share a single slot, so they still get
/// at most one record per window between them.
/// </para>
/// </summary>
internal sealed class PerKeyThrottle(TimeSpan window, TimeProvider? timeProvider = null, int maxKeys = PerKeyThrottle.DefaultMaxKeys)
{
    public const int DefaultMaxKeys = 16 * 1024;
    private const int PruneThreshold = 1024;

    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;
    private readonly ConcurrentDictionary<string, DateTimeOffset> _lastByKey = new(StringComparer.Ordinal);
    private long _nextPruneTicks;
    private long _overflowLastTicks = long.MinValue;

    public int TrackedKeys => _lastByKey.Count;

    /// <summary>True when nothing was let through for this key within the window (and records now).</summary>
    public bool TryAcquire(string key)
    {
        var now = _time.GetUtcNow();
        while (true)
        {
            if (_lastByKey.TryGetValue(key, out var last))
            {
                if (now - last < window)
                    return false;
                if (!_lastByKey.TryUpdate(key, now, last))
                    continue;
            }
            else
            {
                if (_lastByKey.Count >= maxKeys)
                {
                    PruneIfDue(now);
                    if (_lastByKey.Count >= maxKeys)
                        return TryAcquireOverflow(now);
                }

                if (!_lastByKey.TryAdd(key, now))
                    continue;
            }

            PruneIfDue(now);
            return true;
        }
    }

    /// <summary>The shared slot for untracked keys while the table is full.</summary>
    private bool TryAcquireOverflow(DateTimeOffset now)
    {
        while (true)
        {
            var last = Interlocked.Read(ref _overflowLastTicks);
            if (last != long.MinValue && now.UtcTicks - last < window.Ticks)
                return false;
            if (Interlocked.CompareExchange(ref _overflowLastTicks, now.UtcTicks, last) == last)
                return true;
        }
    }

    /// <summary>Removes expired keys once the table is large, scanning it at most once per window.</summary>
    private void PruneIfDue(DateTimeOffset now)
    {
        if (_lastByKey.Count <= PruneThreshold)
            return;

        var due = Interlocked.Read(ref _nextPruneTicks);
        if (now.UtcTicks < due)
            return;
        if (Interlocked.CompareExchange(ref _nextPruneTicks, now.UtcTicks + window.Ticks, due) != due)
            return; // Another caller is pruning.

        foreach (var entry in _lastByKey)
        {
            if (now - entry.Value >= window)
                ((ICollection<KeyValuePair<string, DateTimeOffset>>)_lastByKey).Remove(entry);
        }
    }
}

/// <summary>Which routed messages need an established pair, kept free of sockets so they can be tested directly.</summary>
internal static class RoutingRules
{
    /// <summary>
    /// Messages forwarded (or queued) only between keys with an established pair, or to oneself. Also re-checked when
    /// a queued one is delivered. <c>pairRemove</c> only removes state, so it stays unconditional and half-pairs heal.
    /// <c>ownerRevoked</c> is delivered or queued only between paired keys (02-D28), but a queued one isn't re-checked:
    /// the client's pairRemove right after it removes the pair before it's delivered.
    /// </summary>
    public static bool RequiresPair(string msgType) => msgType is
        MessageTypes.Wind or MessageTypes.WindRequest or MessageTypes.Unwind
        or MessageTypes.OwnerSettingsQuery or MessageTypes.OwnerSettingsUpdate or MessageTypes.OwnerGrant
        or MessageTypes.Call;

    /// <summary>
    /// Whether <paramref name="fromKey"/> may address <paramref name="toKey"/>: a message to oneself (the debug
    /// self-pair) always may; otherwise only with an established pair. <paramref name="hasPair"/> is asked only then.
    /// </summary>
    public static bool MayRoute(string fromKey, string toKey, Func<bool> hasPair) =>
        string.Equals(fromKey, toKey, StringComparison.Ordinal) || hasPair();
}
