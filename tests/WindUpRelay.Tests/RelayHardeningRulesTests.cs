using Microsoft.Extensions.Configuration;
using WindUpKey.Protocol;

namespace WindUpRelay.Tests;

/// <summary>
/// The pure parts of 05: the first-bind limiter and the register policy around it (05-D2), the table of live settings
/// queries (05-D1 (a)), and the keep-alive configuration (05-D3).
/// </summary>
public class RelayHardeningRulesTests
{
    private const string ClientA = "203.0.113.7";
    private const string ClientB = "198.51.100.1";

    private sealed class ManualTime : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = new(2026, 10, 13, 12, 0, 0, TimeSpan.Zero);
        public override DateTimeOffset GetUtcNow() => Now;
    }

    private static int AcquireAll(FirstBindLimiter limiter, string? client, int attempts)
    {
        var granted = 0;
        for (var i = 0; i < attempts; i++)
        {
            if (limiter.TryAcquire(client, out _))
                granted++;
        }

        return granted;
    }

    [Fact]
    public void FirstBindLimiter_AllowsTheHourlyLimit_ThenRefuses_UntilTheHourHasPassed()
    {
        var time = new ManualTime();
        var limiter = new FirstBindLimiter(5, 20, time);

        Assert.Equal(5, AcquireAll(limiter, ClientA, 6));
        Assert.True(limiter.TryAcquire(ClientB, out _), "another address has its own limit");

        time.Now += TimeSpan.FromMinutes(59);
        Assert.False(limiter.TryAcquire(ClientA, out _));
        time.Now += TimeSpan.FromMinutes(1);
        Assert.Equal(5, AcquireAll(limiter, ClientA, 6));
    }

    [Fact]
    public void FirstBindLimiter_AllowsTheDailyLimit_ThenRefuses_UntilTheDayHasPassed()
    {
        var time = new ManualTime();
        var limiter = new FirstBindLimiter(5, 20, time);
        var start = time.Now;

        for (var hour = 0; hour < 4; hour++)
        {
            Assert.Equal(5, AcquireAll(limiter, ClientA, 6));
            time.Now += TimeSpan.FromHours(1);
        }

        // 20 in 4 hours: a new hour doesn't help until the first ones are a day old.
        Assert.False(limiter.TryAcquire(ClientA, out _));
        time.Now = start + TimeSpan.FromHours(23.9);
        Assert.False(limiter.TryAcquire(ClientA, out _));
        time.Now = start + TimeSpan.FromDays(1);
        Assert.Equal(5, AcquireAll(limiter, ClientA, 6));
    }

    [Fact]
    public void FirstBindLimiter_NeverLimitsANullClientKey()
    {
        var limiter = new FirstBindLimiter(1, 1, new ManualTime());

        Assert.Equal(50, AcquireAll(limiter, null, 50));
        Assert.Equal(0, limiter.TrackedKeys);
    }

    [Fact]
    public void FirstBindLimiter_ARefundGivesTheSlotBack()
    {
        var limiter = new FirstBindLimiter(2, 20, new ManualTime());
        Assert.True(limiter.TryAcquire(ClientA, out _));
        Assert.True(limiter.TryAcquire(ClientA, out var lost));

        limiter.Refund(lost);

        Assert.True(limiter.TryAcquire(ClientA, out _));
        Assert.False(limiter.TryAcquire(ClientA, out _));
    }

    [Theory]
    [InlineData(0, 0, 100)]
    [InlineData(0, 3, 3)]
    [InlineData(3, 0, 3)]
    public void FirstBindLimiter_AZeroLimitIsOff(int perHour, int perDay, int expected)
    {
        var limiter = new FirstBindLimiter(perHour, perDay, new ManualTime());

        Assert.Equal(expected, AcquireAll(limiter, ClientA, 100));
    }

    [Fact]
    public void FirstBindLimiter_StaysBounded_AndAFullTableLetsNewAddressesThrough()
    {
        var time = new ManualTime();
        var limiter = new FirstBindLimiter(1, 20, time, maxKeys: 4);
        for (var i = 0; i < 4; i++)
            Assert.True(limiter.TryAcquire("192.0.2." + i, out _));

        // 05-D7: the table is full, so an address it can't track gets no limit (as before 05), never a shared bucket;
        // the table doesn't grow, and the tracked addresses keep theirs.
        Assert.Equal(3, AcquireAll(limiter, "192.0.2.100", 3));
        Assert.Equal(3, AcquireAll(limiter, "192.0.2.101", 3));
        Assert.True(limiter.TryAcquire("192.0.2.102", out var untracked));
        Assert.Equal(default, untracked);
        Assert.Equal(4, limiter.TrackedKeys);
        Assert.False(limiter.TryAcquire("192.0.2.0", out _));

        // Once the tracked ones have nothing left in the day window, they make room, and new addresses are limited.
        time.Now += TimeSpan.FromDays(1);
        Assert.Equal(1, AcquireAll(limiter, "192.0.2.100", 3));
    }

    [Fact]
    public void FirstBindLimiter_DefaultTable_IsLargerThanBefore05D7()
    {
        Assert.True(FirstBindLimiter.DefaultMaxKeys >= 64 * 1024);
        Assert.Equal(48, FirstBindLimiter.Ipv6PrefixBits);
    }

    private static readonly string Secret = RelaySecret.Generate();
    private static readonly string Hash = RelaySecret.HashHex(Secret)!;

    [Theory]
    [InlineData("Bind")]
    [InlineData("Enforce")]
    public void Resolve_AFirstBindConsumesASlot_AndARefusalIsTryLater_WithNoAccount(string mode)
    {
        var reserved = 0;
        var created = 0;
        var outcome = RegisterAuthPolicy.Resolve(
            Enum.Parse<RelayAuthMode>(mode), Secret, () => null, _ => { created++; return true; }, () => { reserved++; return false; });

        Assert.Equal(RegisterAuthOutcome.TryLater, outcome);
        Assert.Equal(1, reserved);
        Assert.Equal(0, created);
    }

    [Fact]
    public void Resolve_AuthenticatedAuthFailedLegacyAndRefusals_NeverConsume()
    {
        var other = RelaySecret.HashHex(RelaySecret.Generate());
        Func<bool> never = () => throw new InvalidOperationException("consumed a first-bind slot");
        Func<string, bool> noCreate = _ => throw new InvalidOperationException("create");

        Assert.Equal(RegisterAuthOutcome.Authenticated, RegisterAuthPolicy.Resolve(RelayAuthMode.Bind, Secret, () => Hash, noCreate, never));
        Assert.Equal(RegisterAuthOutcome.AuthFailed, RegisterAuthPolicy.Resolve(RelayAuthMode.Bind, Secret, () => other, noCreate, never));
        Assert.Equal(RegisterAuthOutcome.AuthFailed, RegisterAuthPolicy.Resolve(RelayAuthMode.Bind, "not-a-secret", () => null, noCreate, never));
        Assert.Equal(RegisterAuthOutcome.Legacy, RegisterAuthPolicy.Resolve(RelayAuthMode.Bind, null, () => null, noCreate, never));
        Assert.Equal(RegisterAuthOutcome.RefuseSilently, RegisterAuthPolicy.Resolve(RelayAuthMode.Enforce, null, () => null, noCreate, never));
        Assert.Equal(RegisterAuthOutcome.Legacy, RegisterAuthPolicy.Resolve(RelayAuthMode.Off, Secret, () => null, noCreate, never));
    }

    [Fact]
    public void Resolve_ALostRace_RefundsTheSlot_AndDecidesAgainstTheWinner()
    {
        var reads = 0;
        var refunds = 0;
        var outcome = RegisterAuthPolicy.Resolve(
            RelayAuthMode.Bind, Secret, () => reads++ == 0 ? null : Hash, _ => false, () => true, () => refunds++);

        Assert.Equal(RegisterAuthOutcome.Authenticated, outcome);
        Assert.Equal(1, refunds);
    }

    [Fact]
    public void Resolve_TheReDecisionAfterAFailedCreate_NeverBindsWithoutAnAccount()
    {
        // 05-D2's trap: the create failed but no account is there on re-read (e.g. reset in between). Bound here would
        // be a bound session with no account; it must not happen, with or without the limiter.
        var races = 0;
        Assert.Equal(RegisterAuthOutcome.TryLater, RegisterAuthPolicy.Resolve(RelayAuthMode.Bind, Secret, () => null, _ => false, () => true, () => { }, () => races++));
        Assert.Equal(RegisterAuthOutcome.TryLater, RegisterAuthPolicy.Resolve(RelayAuthMode.Enforce, Secret, () => null, _ => false, () => true, () => { }, () => races++));
        Assert.Equal(RegisterAuthOutcome.TryLater, RegisterAuthPolicy.Resolve(RelayAuthMode.Bind, Secret, () => null, _ => false));

        // 05-D10: reported as a lost race, so it's recorded as bind_race, not as the limit.
        Assert.Equal(2, races);
    }

    [Fact]
    public void Resolve_OnlyTheLostRaceRefusal_IsReportedAsABindRace()
    {
        var races = 0;
        void Race() => races++;
        var reads = 0;

        // A limiter refusal, a lost race against the winner (re-decided, not refused), a bind and an auth failure.
        Assert.Equal(RegisterAuthOutcome.TryLater, RegisterAuthPolicy.Resolve(RelayAuthMode.Bind, Secret, () => null, _ => true, () => false, () => { }, Race));
        Assert.Equal(RegisterAuthOutcome.Authenticated, RegisterAuthPolicy.Resolve(RelayAuthMode.Bind, Secret, () => reads++ == 0 ? null : Hash, _ => false, () => true, () => { }, Race));
        Assert.Equal(RegisterAuthOutcome.Bound, RegisterAuthPolicy.Resolve(RelayAuthMode.Bind, Secret, () => null, _ => true, () => true, () => { }, Race));
        Assert.Equal(RegisterAuthOutcome.AuthFailed, RegisterAuthPolicy.Resolve(RelayAuthMode.Bind, Secret, () => RelaySecret.HashHex(RelaySecret.Generate()), _ => true, () => true, () => { }, Race));

        Assert.Equal(0, races);
    }

    [Fact]
    public void Resolve_ACreateThatThrows_RefundsTheSlot_AndThrows()
    {
        // 05-D11: no account was created, so the slot goes back; the failure still reaches the caller.
        var refunds = 0;
        var races = 0;
        Assert.Throws<InvalidOperationException>(() => RegisterAuthPolicy.Resolve(
            RelayAuthMode.Bind, Secret, () => null, _ => throw new InvalidOperationException("disk"), () => true, () => refunds++, () => races++));

        Assert.Equal(1, refunds);
        Assert.Equal(0, races);
    }

    [Fact]
    public void Resolve_ACreateThatThrows_GivesTheRealLimiterItsSlotBack()
    {
        var limiter = new FirstBindLimiter(1, 20, new ManualTime());
        var lease = default(FirstBindLease);
        Assert.ThrowsAny<Exception>(() => RegisterAuthPolicy.Resolve(
            RelayAuthMode.Bind, Secret, () => null, _ => throw new IOException("disk"),
            () => limiter.TryAcquire(ClientA, out lease), () => limiter.Refund(lease)));

        Assert.True(limiter.TryAcquire(ClientA, out _), "the slot came back");
    }

    [Fact]
    public void Resolve_AgainstARealStoreAndLimiter_OnlyCreationsCount()
    {
        var dir = Path.Combine(Path.GetTempPath(), "windup-relay-tests-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var store = new RelayStore(Path.Combine(dir, "relay.db"));
            var limiter = new FirstBindLimiter(2, 20, new ManualTime());
            RegisterAuthOutcome Register(string key, string secret)
            {
                var lease = default(FirstBindLease);
                return RegisterAuthPolicy.Resolve(
                    RelayAuthMode.Bind,
                    secret,
                    () => store.GetAccountSecretHash(key),
                    h => store.TryCreateAccount(key, h),
                    () => limiter.TryAcquire(ClientA, out lease),
                    () => limiter.Refund(lease));
            }

            var secrets = Enumerable.Range(0, 3).Select(_ => RelaySecret.Generate()).ToList();
            Assert.Equal(RegisterAuthOutcome.Bound, Register("KEYAAAA1", secrets[0]));
            Assert.Equal(RegisterAuthOutcome.Authenticated, Register("KEYAAAA1", secrets[0]));
            Assert.Equal(RegisterAuthOutcome.AuthFailed, Register("KEYAAAA1", secrets[1]));
            Assert.Equal(RegisterAuthOutcome.Bound, Register("KEYAAAA2", secrets[1]));

            Assert.Equal(RegisterAuthOutcome.TryLater, Register("KEYAAAA3", secrets[2]));
            Assert.Null(store.GetAccountSecretHash("KEYAAAA3"));
            Assert.Equal(RegisterAuthOutcome.Authenticated, Register("KEYAAAA2", secrets[1]));
        }
        finally
        {
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void RequestedReplies_AreUsedOnce_ExpireAfterTwoMinutes_AndMatchAllThreeParts()
    {
        var time = new ManualTime();
        var table = new RequestedReplies(time);
        table.Add("rq", "OWNERAA1", "DOLLBBB2");

        Assert.False(table.TryConsume("rq", "DOLLBBB2", "OWNERAA1"), "the other direction");
        Assert.False(table.TryConsume("rq", "OWNERAA1", "THIRDCC3"), "another doll");
        Assert.False(table.TryConsume("other", "OWNERAA1", "DOLLBBB2"), "another request");
        Assert.True(table.TryConsume("rq", "OWNERAA1", "DOLLBBB2"));
        Assert.False(table.TryConsume("rq", "OWNERAA1", "DOLLBBB2"), "used once");

        table.Add("late", "OWNERAA1", "DOLLBBB2");
        time.Now += RequestedReplies.Ttl;
        Assert.False(table.TryConsume("late", "OWNERAA1", "DOLLBBB2"));
        Assert.Equal(0, table.Count);
    }

    [Fact]
    public void RequestedReplies_AWithdrawnEntryDoesNotMatch()
    {
        var table = new RequestedReplies(new ManualTime());
        table.Add("rq", "OWNERAA1", "DOLLBBB2");
        table.Remove("rq", "OWNERAA1", "DOLLBBB2");

        Assert.False(table.TryConsume("rq", "OWNERAA1", "DOLLBBB2"));
    }

    [Fact]
    public void RequestedReplies_StayBounded_DroppingTheOldestFirst()
    {
        var time = new ManualTime();
        var table = new RequestedReplies(time, maxEntries: 3);
        for (var i = 0; i < 5; i++)
        {
            table.Add("rq" + i, "OWNERAA1", "DOLLBBB2");
            time.Now += TimeSpan.FromSeconds(1);
        }

        Assert.Equal(3, table.Count);
        Assert.False(table.TryConsume("rq0", "OWNERAA1", "DOLLBBB2"));
        Assert.False(table.TryConsume("rq1", "OWNERAA1", "DOLLBBB2"));
        Assert.True(table.TryConsume("rq4", "OWNERAA1", "DOLLBBB2"));
    }

    private static KeepAliveSettings KeepAlive(string? interval, string? timeout)
    {
        var values = new Dictionary<string, string?>();
        if (interval is not null)
            values["Relay:KeepAliveIntervalSeconds"] = interval;
        if (timeout is not null)
            values["Relay:KeepAliveTimeoutSeconds"] = timeout;
        return RelayWebSocketEndpoint.KeepAliveFromConfig(new ConfigurationBuilder().AddInMemoryCollection(values).Build());
    }

    [Fact]
    public void KeepAlive_DefaultsTo30And90_TheClientsOwn()
    {
        var settings = KeepAlive(null, null);
        var options = RelayWebSocketEndpoint.BuildWebSocketOptions(settings);

        Assert.Equal(new KeepAliveSettings(30, 90), settings);
        Assert.Equal(WireLimits.KeepAliveInterval, options.KeepAliveInterval);
        Assert.Equal(WireLimits.KeepAliveTimeout, options.KeepAliveTimeout);
        Assert.Equal(new KeepAliveSettings(30, 90), KeepAlive("soon", "-"));
    }

    [Fact]
    public void KeepAlive_ConfiguredValues_AreUsedAsGiven()
    {
        var options = RelayWebSocketEndpoint.BuildWebSocketOptions(KeepAlive("1", "2"));

        Assert.Equal(TimeSpan.FromSeconds(1), options.KeepAliveInterval);
        Assert.Equal(TimeSpan.FromSeconds(2), options.KeepAliveTimeout);
    }

    [Theory]
    [InlineData("-1", "90", 30, 90)]
    [InlineData("30", "-5", 30, 90)]
    [InlineData("-30", "-90", 30, 90)]
    [InlineData("0", "-1", 0, 90)]
    public void KeepAlive_NegativeValues_GetTheDefault(string interval, string timeout, int expectedInterval, int expectedTimeout)
    {
        // 05-D11: only 0 means off.
        Assert.Equal(new KeepAliveSettings(expectedInterval, expectedTimeout), KeepAlive(interval, timeout));
    }

    private static FirstBindLimiter FirstBinds(string? perHour, string? perDay)
    {
        var values = new Dictionary<string, string?>();
        if (perHour is not null)
            values["Relay:FirstBindsPerIpPerHour"] = perHour;
        if (perDay is not null)
            values["Relay:FirstBindsPerIpPerDay"] = perDay;
        return RelayWebSocketEndpoint.FirstBindsFromConfig(new ConfigurationBuilder().AddInMemoryCollection(values).Build());
    }

    [Theory]
    [InlineData(null, null, 5, 20)]
    [InlineData("3", "7", 3, 7)]
    [InlineData("0", "20", 0, 20)]
    [InlineData("5", "0", 5, 0)]
    [InlineData("0", "0", 0, 0)]
    [InlineData("-1", "-20", 5, 20)]
    [InlineData("soon", "", 5, 20)]
    [InlineData("1000000", "1000000", 100_000, 100_000)]
    public void FirstBinds_FromConfig_DefaultsTo5And20_ZeroIsOff_AndNegativesGetTheDefault(
        string? perHour, string? perDay, int expectedPerHour, int expectedPerDay)
    {
        var limiter = FirstBinds(perHour, perDay);

        Assert.Equal(expectedPerHour, limiter.PerHour);
        Assert.Equal(expectedPerDay, limiter.PerDay);
        Assert.Equal(expectedPerHour > 0 || expectedPerDay > 0, limiter.Enabled);
    }

    [Fact]
    public void TheShippedAppsettings_TurnsTheBindLimitOff_UntilTheFunnelCheckPasses()
    {
        // 05-D8: off in the shipped file; the code defaults (5 and 20) apply only when a value is missing.
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "WindUpRelay.csproj")))
            dir = dir.Parent;
        Assert.NotNull(dir);

        var shipped = new ConfigurationBuilder().AddJsonFile(Path.Combine(dir!.FullName, "appsettings.json")).Build();
        Assert.Equal("0", shipped["Relay:FirstBindsPerIpPerHour"]);
        Assert.Equal("0", shipped["Relay:FirstBindsPerIpPerDay"]);
        Assert.False(RelayWebSocketEndpoint.FirstBindsFromConfig(shipped).Enabled);
    }

    [Fact]
    public void KeepAlive_ZeroTurnsItOff()
    {
        var noPings = RelayWebSocketEndpoint.BuildWebSocketOptions(KeepAlive("0", "90"));
        Assert.Equal(TimeSpan.Zero, noPings.KeepAliveInterval);
        Assert.Equal(Timeout.InfiniteTimeSpan, noPings.KeepAliveTimeout);

        var noTimeout = RelayWebSocketEndpoint.BuildWebSocketOptions(KeepAlive("30", "0"));
        Assert.Equal(TimeSpan.FromSeconds(30), noTimeout.KeepAliveInterval);
        Assert.Equal(Timeout.InfiniteTimeSpan, noTimeout.KeepAliveTimeout);
    }
}
