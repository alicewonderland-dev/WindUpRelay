using System.Collections.Concurrent;
using WindUpKey.Protocol;

namespace WindUpRelay.Tests;

public class SessionRulesTests
{
    private const string Key = "ABCD1234";

    private sealed class FakeSession(string name, bool open = true)
    {
        public string Name { get; } = name;
        public bool Open { get; set; } = open;
        public override string ToString() => Name;
    }

    private static ConcurrentDictionary<string, FakeSession> NewMap() => new(StringComparer.Ordinal);

    private static bool Claim(
        ConcurrentDictionary<string, FakeSession> map,
        FakeSession session,
        bool authenticated,
        out FakeSession? displaced)
        => SessionMap.TryClaim(map, Key, session, authenticated, s => s.Open, out displaced);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void FreeKey_IsClaimed(bool authenticated)
    {
        var map = NewMap();
        var a = new FakeSession("a");

        Assert.True(Claim(map, a, authenticated, out var displaced));
        Assert.Null(displaced);
        Assert.Same(a, map[Key]);
    }

    [Fact]
    public void Legacy_AgainstOpenHolder_Collides_AndLeavesTheMapAlone()
    {
        var map = NewMap();
        var holder = new FakeSession("holder");
        map[Key] = holder;

        Assert.False(Claim(map, new FakeSession("legacy"), authenticated: false, out var displaced));
        Assert.Null(displaced);
        Assert.Same(holder, map[Key]);
    }

    [Fact]
    public void Legacy_AgainstClosedHolder_TakesTheKey_WithoutDisplacing()
    {
        var map = NewMap();
        map[Key] = new FakeSession("dead", open: false);
        var legacy = new FakeSession("legacy");

        Assert.True(Claim(map, legacy, authenticated: false, out var displaced));
        Assert.Null(displaced);
        Assert.Same(legacy, map[Key]);
    }

    [Fact]
    public void Authenticated_AgainstOpenHolder_TakesOver_AndReportsTheHolder()
    {
        var map = NewMap();
        var holder = new FakeSession("holder");
        map[Key] = holder;
        var claimant = new FakeSession("claimant");

        Assert.True(Claim(map, claimant, authenticated: true, out var displaced));
        Assert.Same(holder, displaced);
        // The new session is already mapped when the caller learns whom to close.
        Assert.Same(claimant, map[Key]);
    }

    [Fact]
    public void ReRegister_OfTheHolder_IsANoOp()
    {
        var map = NewMap();
        var a = new FakeSession("a");
        map[Key] = a;

        Assert.True(Claim(map, a, authenticated: false, out var displaced));
        Assert.Null(displaced);
        Assert.Same(a, map[Key]);
    }

    [Fact]
    public void ConcurrentAuthenticatedClaims_DisplaceEverySessionThatLost()
    {
        // Every claimant either ends up holding the key or is reported as displaced exactly once,
        // so no displaced session is left open still believing it owns the key.
        for (var round = 0; round < 200; round++)
        {
            var map = NewMap();
            var sessions = Enumerable.Range(0, 8).Select(i => new FakeSession("s" + i)).ToArray();
            var displacedBag = new ConcurrentBag<FakeSession>();
            using var start = new Barrier(sessions.Length);

            Parallel.ForEach(sessions, new ParallelOptions { MaxDegreeOfParallelism = sessions.Length }, s =>
            {
                start.SignalAndWait();
                Assert.True(Claim(map, s, authenticated: true, out var displaced));
                if (displaced is not null)
                    displacedBag.Add(displaced);
            });

            var holder = map[Key];
            var displacedList = displacedBag.ToList();
            Assert.Equal(sessions.Length - 1, displacedList.Count);
            Assert.Equal(displacedList.Count, displacedList.Distinct().Count());
            Assert.DoesNotContain(holder, displacedList);
        }
    }

    private sealed class ManualTime : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = new(2026, 10, 11, 12, 0, 0, TimeSpan.Zero);
        public override DateTimeOffset GetUtcNow() => Now;
    }

    [Fact]
    public void PerKeyThrottle_LetsOneThroughPerKeyPerWindow()
    {
        var time = new ManualTime();
        var throttle = new PerKeyThrottle(TimeSpan.FromMinutes(5), time);

        Assert.True(throttle.TryAcquire(Key));
        Assert.False(throttle.TryAcquire(Key));
        Assert.True(throttle.TryAcquire("WXYZ9876"));

        time.Now += TimeSpan.FromMinutes(4);
        Assert.False(throttle.TryAcquire(Key));

        time.Now += TimeSpan.FromMinutes(1);
        Assert.True(throttle.TryAcquire(Key));
        Assert.False(throttle.TryAcquire(Key));
    }

    [Fact]
    public void PerKeyThrottle_PrunesExpiredKeys()
    {
        var time = new ManualTime();
        var throttle = new PerKeyThrottle(TimeSpan.FromMinutes(5), time);
        for (var i = 0; i < 1000; i++)
            throttle.TryAcquire("K" + i);

        time.Now += TimeSpan.FromMinutes(10);
        for (var i = 0; i < 100; i++)
            throttle.TryAcquire("N" + i);

        Assert.True(throttle.TrackedKeys < 1000);
    }

    [Fact]
    public void PerKeyThrottle_ScansForExpiredKeysAtMostOncePerWindow()
    {
        var time = new ManualTime();
        var throttle = new PerKeyThrottle(TimeSpan.FromMinutes(5), time);
        var start = time.Now;

        // Crossing the threshold at t0 prunes (nothing expired) and schedules the next prune for t0+5m.
        for (var i = 0; i < 1025; i++)
            throttle.TryAcquire("A" + i);
        time.Now = start + TimeSpan.FromMinutes(1);
        for (var i = 0; i < 1025; i++)
            throttle.TryAcquire("C" + i);

        // t0+5m: the A keys have expired and go; the C keys haven't yet.
        time.Now = start + TimeSpan.FromMinutes(5);
        throttle.TryAcquire("D");
        Assert.Equal(1025 + 1, throttle.TrackedKeys);

        // t0+6m: the C keys have expired too, but the next scan isn't due until t0+10m.
        time.Now = start + TimeSpan.FromMinutes(6);
        throttle.TryAcquire("E");
        Assert.Equal(1025 + 2, throttle.TrackedKeys);

        time.Now = start + TimeSpan.FromMinutes(10);
        throttle.TryAcquire("F");
        Assert.Equal(2, throttle.TrackedKeys); // E and F; C and D have expired.
    }

    [Fact]
    public void PerKeyThrottle_WhenFull_StaysCapped_AndUntrackedKeysShareOneSlot()
    {
        var time = new ManualTime();
        var throttle = new PerKeyThrottle(TimeSpan.FromMinutes(5), time, maxKeys: 1100);
        for (var i = 0; i < 1100; i++)
            Assert.True(throttle.TryAcquire("K" + i));

        // Full and nothing expired: one record between all new keys per window, and nothing more is tracked.
        Assert.True(throttle.TryAcquire("NEW1"));
        Assert.False(throttle.TryAcquire("NEW2"));
        Assert.False(throttle.TryAcquire("NEW1"));
        Assert.Equal(1100, throttle.TrackedKeys);

        // Tracked keys keep their own window.
        Assert.False(throttle.TryAcquire("K0"));

        // Once the window passes, expired keys are pruned and new keys get their own slots again.
        time.Now += TimeSpan.FromMinutes(5);
        Assert.True(throttle.TryAcquire("NEW2"));
        Assert.True(throttle.TryAcquire("NEW3"));
        Assert.False(throttle.TryAcquire("NEW3"));
        Assert.Equal(2, throttle.TrackedKeys);
    }
}

public sealed class RoutingRulesTests
{
    [Theory]
    [InlineData(MessageTypes.Wind)]
    [InlineData(MessageTypes.WindRequest)]
    [InlineData(MessageTypes.Unwind)]
    [InlineData(MessageTypes.OwnerSettingsQuery)]
    [InlineData(MessageTypes.OwnerSettingsUpdate)]
    [InlineData(MessageTypes.OwnerGrant)]
    [InlineData(MessageTypes.Call)]
    public void Forwards_RequireAPair(string type) => Assert.True(RoutingRules.RequiresPair(type));

    [Theory]
    [InlineData(MessageTypes.OwnerRevoked)]
    [InlineData(MessageTypes.PairRemove)]
    [InlineData(MessageTypes.PairEstablished)]
    public void RemovalNotices_AndPairing_DoNot(string type) => Assert.False(RoutingRules.RequiresPair(type));

    [Fact]
    public void MayRoute_ToOneself_WithoutAskingForAPair()
    {
        Assert.True(RoutingRules.MayRoute("AAAA1111", "AAAA1111", () => throw new InvalidOperationException()));
        Assert.True(RoutingRules.MayRoute("AAAA1111", "BBBB2222", () => true));
        Assert.False(RoutingRules.MayRoute("AAAA1111", "BBBB2222", () => false));
    }
}
