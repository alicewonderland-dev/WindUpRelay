using WindUpKey.Protocol;

namespace WindUpRelay.Tests;

public class RegisterAuthPolicyTests
{
    private static readonly string Secret = RelaySecret.Generate();
    private static readonly string StoredHash = RelaySecret.HashHex(Secret)!;
    private static readonly string OtherSecret = RelaySecret.Generate();
    private const string Malformed = "not-a-secret";

    // Test signatures take mode names: xUnit test methods are public, the relay's types are internal.
    private static RelayAuthMode Mode(string name) => Enum.Parse<RelayAuthMode>(name);

    private static RegisterAuthOutcome Decide(RelayAuthMode mode, bool claimed, string? secret)
        => RegisterAuthPolicy.Decide(mode, claimed ? StoredHash : null, secret);

    private static RegisterAuthOutcome Decide(string mode, bool claimed, string? secret)
        => Decide(Mode(mode), claimed, secret);

    // Off: secrets are ignored entirely.
    [Theory]
    [InlineData(false, null)]
    [InlineData(true, null)]
    [InlineData(false, "valid")]
    [InlineData(true, "valid")]
    [InlineData(true, "other")]
    [InlineData(true, "malformed")]
    public void Off_AlwaysLegacy(bool claimed, string? which)
    {
        Assert.Equal(RegisterAuthOutcome.Legacy, Decide(RelayAuthMode.Off, claimed, Pick(which)));
    }

    [Fact]
    public void Bind_NoSecret_UnclaimedKey_IsLegacy()
    {
        Assert.Equal(RegisterAuthOutcome.Legacy, Decide(RelayAuthMode.Bind, claimed: false, null));
        Assert.Equal(RegisterAuthOutcome.Legacy, Decide(RelayAuthMode.Bind, claimed: false, ""));
    }

    [Fact]
    public void Bind_NoSecret_ClaimedKey_RefusesSilently()
    {
        Assert.Equal(RegisterAuthOutcome.RefuseSilently, Decide(RelayAuthMode.Bind, claimed: true, null));
        Assert.Equal(RegisterAuthOutcome.RefuseSilently, Decide(RelayAuthMode.Bind, claimed: true, ""));
    }

    [Theory]
    [InlineData(nameof(RelayAuthMode.Bind))]
    [InlineData(nameof(RelayAuthMode.Enforce))]
    public void ValidSecret_UnclaimedKey_Binds(string mode)
    {
        Assert.Equal(RegisterAuthOutcome.Bound, Decide(mode, claimed: false, Secret));
    }

    [Theory]
    [InlineData(nameof(RelayAuthMode.Bind))]
    [InlineData(nameof(RelayAuthMode.Enforce))]
    public void MatchingSecret_ClaimedKey_Authenticates(string mode)
    {
        Assert.Equal(RegisterAuthOutcome.Authenticated, Decide(mode, claimed: true, Secret));
    }

    [Theory]
    [InlineData(nameof(RelayAuthMode.Bind))]
    [InlineData(nameof(RelayAuthMode.Enforce))]
    public void WrongSecret_ClaimedKey_Fails(string mode)
    {
        Assert.Equal(RegisterAuthOutcome.AuthFailed, Decide(mode, claimed: true, OtherSecret));
    }

    [Theory]
    [InlineData(nameof(RelayAuthMode.Bind), false)]
    [InlineData(nameof(RelayAuthMode.Bind), true)]
    [InlineData(nameof(RelayAuthMode.Enforce), false)]
    [InlineData(nameof(RelayAuthMode.Enforce), true)]
    public void MalformedSecret_Fails(string mode, bool claimed)
    {
        Assert.Equal(RegisterAuthOutcome.AuthFailed, Decide(mode, claimed, Malformed));
    }

    [Theory]
    [InlineData(false, null)]
    [InlineData(false, "")]
    [InlineData(true, null)]
    [InlineData(true, "")]
    public void Enforce_NoSecret_RefusesSilently(bool claimed, string? secret)
    {
        Assert.Equal(RegisterAuthOutcome.RefuseSilently, Decide(RelayAuthMode.Enforce, claimed, secret));
    }

    [Theory]
    [InlineData(null, nameof(RelayAuthMode.Bind))]
    [InlineData("", nameof(RelayAuthMode.Bind))]
    [InlineData("  ", nameof(RelayAuthMode.Bind))]
    [InlineData("Off", nameof(RelayAuthMode.Off))]
    [InlineData("off", nameof(RelayAuthMode.Off))]
    [InlineData("Bind", nameof(RelayAuthMode.Bind))]
    [InlineData(" bind ", nameof(RelayAuthMode.Bind))]
    [InlineData("Enforce", nameof(RelayAuthMode.Enforce))]
    [InlineData("ENFORCE", nameof(RelayAuthMode.Enforce))]
    public void TryParseMode_Accepts(string? raw, string expected)
    {
        Assert.True(RegisterAuthPolicy.TryParseMode(raw, out var mode));
        Assert.Equal(Mode(expected), mode);
    }

    [Theory]
    [InlineData("0")]
    [InlineData("1")]
    [InlineData("Strict")]
    [InlineData("Offf")]
    [InlineData("Bind,Enforce")]
    public void TryParseMode_RejectsUnknown(string raw)
    {
        Assert.False(RegisterAuthPolicy.TryParseMode(raw, out _));
    }

    private static string? Pick(string? which) => which switch
    {
        "valid" => Secret,
        "other" => OtherSecret,
        "malformed" => Malformed,
        _ => which,
    };
}

public class RegisterAuthResolveTests
{
    private static readonly string Secret = RelaySecret.Generate();
    private static readonly string Hash = RelaySecret.HashHex(Secret)!;

    [Fact]
    public void Off_NeverTouchesTheStore()
    {
        var outcome = RegisterAuthPolicy.Resolve(
            RelayAuthMode.Off,
            Secret,
            () => throw new InvalidOperationException("read"),
            _ => throw new InvalidOperationException("create"));
        Assert.Equal(RegisterAuthOutcome.Legacy, outcome);
    }

    [Fact]
    public void FirstBind_CreatesAccountWithHashOfSecret()
    {
        string? created = null;
        var outcome = RegisterAuthPolicy.Resolve(
            RelayAuthMode.Bind, Secret, () => created, h => { created = h; return true; });

        Assert.Equal(RegisterAuthOutcome.Bound, outcome);
        Assert.Equal(Hash, created);
    }

    [Fact]
    public void ClaimedKey_DoesNotCreate()
    {
        var outcome = RegisterAuthPolicy.Resolve(
            RelayAuthMode.Enforce, Secret, () => Hash, _ => throw new InvalidOperationException("create"));
        Assert.Equal(RegisterAuthOutcome.Authenticated, outcome);
    }

    [Fact]
    public void LostRace_ToSameSecret_Authenticates()
    {
        var reads = 0;
        var outcome = RegisterAuthPolicy.Resolve(
            RelayAuthMode.Bind, Secret, () => reads++ == 0 ? null : Hash, _ => false);
        Assert.Equal(RegisterAuthOutcome.Authenticated, outcome);
    }

    [Fact]
    public void LostRace_ToOtherSecret_Fails()
    {
        var winner = RelaySecret.HashHex(RelaySecret.Generate());
        var reads = 0;
        var outcome = RegisterAuthPolicy.Resolve(
            RelayAuthMode.Bind, Secret, () => reads++ == 0 ? null : winner, _ => false);
        Assert.Equal(RegisterAuthOutcome.AuthFailed, outcome);
    }

    [Fact]
    public void AgainstRealStore_SecondFirstBindWithOtherSecretFails()
    {
        var dir = Path.Combine(Path.GetTempPath(), "windup-relay-tests-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var store = new RelayStore(Path.Combine(dir, "relay.db"));
            RegisterAuthOutcome Register(string secret) => RegisterAuthPolicy.Resolve(
                RelayAuthMode.Bind,
                secret,
                () => store.GetAccountSecretHash("ABCD1234"),
                h => store.TryCreateAccount("ABCD1234", h));

            Assert.Equal(RegisterAuthOutcome.Bound, Register(Secret));
            Assert.Equal(RegisterAuthOutcome.AuthFailed, Register(RelaySecret.Generate()));
            Assert.Equal(RegisterAuthOutcome.Authenticated, Register(Secret));
        }
        finally
        {
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
            Directory.Delete(dir, recursive: true);
        }
    }
}
