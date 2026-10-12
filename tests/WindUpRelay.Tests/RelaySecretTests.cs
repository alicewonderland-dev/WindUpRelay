using System.Security.Cryptography;
using System.Text;
using WindUpKey.Protocol;

namespace WindUpRelay.Tests;

public class RelaySecretTests
{
    // 32 zero bytes: unpadded base64url is 43 'A's; SHA-256 of the bytes is a well-known vector.
    private static readonly string ZeroSecret = new('A', 43);
    private const string ZeroBytesSha256 = "66687aadf862bd776c8fc18b8e9f8e20089714856ee233b3902a591d0d5f2925";

    [Fact]
    public void Generate_ProducesCanonicalUnpaddedBase64Url()
    {
        var secret = RelaySecret.Generate();

        Assert.Equal(RelaySecret.EncodedLength, secret.Length);
        Assert.All(secret, c => Assert.True(char.IsAsciiLetterOrDigit(c) || c is '-' or '_'));
        Assert.True(RelaySecret.IsValid(secret));
        Assert.True(RelaySecret.TryDecode(secret, out var bytes));
        Assert.Equal(RelaySecret.ByteLength, bytes.Length);
    }

    [Fact]
    public void Generate_IsRandom()
    {
        var secrets = Enumerable.Range(0, 50).Select(_ => RelaySecret.Generate()).ToHashSet();
        Assert.Equal(50, secrets.Count);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")] // 42 chars
    [InlineData("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")] // 44 chars
    [InlineData("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")] // padded
    [InlineData("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA+")] // standard base64 char
    [InlineData("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/")]
    [InlineData("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA!")]
    [InlineData("+AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")] // standard alphabet, canonical tail
    [InlineData("/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")]
    [InlineData("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB")] // non-canonical trailing bits
    [InlineData(" AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")] // unnormalized whitespace
    public void IsValid_RejectsNonCanonical(string? secret)
    {
        Assert.False(RelaySecret.IsValid(secret));
        Assert.Null(RelaySecret.HashHex(secret));
    }

    [Fact]
    public void IsValid_AcceptsCanonical()
    {
        Assert.True(RelaySecret.IsValid(ZeroSecret));
        Assert.True(RelaySecret.IsValid("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAE")); // low bits zero
    }

    [Fact]
    public void Normalize_StripsWhitespaceAndPaddingAndMapsStandardAlphabet()
    {
        var bytes = RandomNumberGenerator.GetBytes(32);
        bytes[0] = 0xFB; // force '+' / '/' style characters in standard base64
        bytes[1] = 0xFF;
        var canonical = System.Buffers.Text.Base64Url.EncodeToString(bytes);
        var standard = Convert.ToBase64String(bytes); // padded, uses + and /
        Assert.Contains('=', standard);
        Assert.Contains('+', standard);
        Assert.Contains('/', standard);

        var pasted = "  " + standard[..20] + "\r\n" + standard[20..] + "\t ";
        var normalized = RelaySecret.Normalize(pasted);

        Assert.Equal(canonical, normalized);
        Assert.True(RelaySecret.IsValid(normalized));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   \r\n")]
    public void Normalize_BlankIsEmpty(string? input)
    {
        Assert.Equal(string.Empty, RelaySecret.Normalize(input));
    }

    [Fact]
    public void Normalize_LeavesCanonicalUnchanged()
    {
        var secret = RelaySecret.Generate();
        Assert.Equal(secret, RelaySecret.Normalize(secret));
    }

    [Fact]
    public void HashHex_IsLowercaseSha256OfDecodedBytes()
    {
        var hash = RelaySecret.HashHex(ZeroSecret);

        Assert.Equal(ZeroBytesSha256, hash);
        Assert.NotEqual(
            Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(ZeroSecret))),
            hash);
    }

    [Fact]
    public void HashHex_DiffersPerSecret()
    {
        Assert.NotEqual(RelaySecret.HashHex(RelaySecret.Generate()), RelaySecret.HashHex(RelaySecret.Generate()));
    }

    [Fact]
    public void HashEquals_ComparesValues()
    {
        var a = RelaySecret.HashHex(RelaySecret.Generate())!;
        var b = RelaySecret.HashHex(RelaySecret.Generate())!;

        Assert.True(RelaySecret.HashEquals(a, a));
        Assert.True(RelaySecret.HashEquals(a, a.ToUpperInvariant()));
        Assert.False(RelaySecret.HashEquals(a, b));
        Assert.False(RelaySecret.HashEquals(a, a[..^1]));
        Assert.False(RelaySecret.HashEquals(a, null));
        Assert.False(RelaySecret.HashEquals(null, a));
    }

    [Theory]
    [InlineData(ErrorCodes.AuthFailed, true)]
    [InlineData(ErrorCodes.AuthRequired, true)]
    [InlineData(ErrorCodes.SessionReplaced, true)]
    [InlineData(ErrorCodes.PairKeyCollision, true)]
    [InlineData(ErrorCodes.RateLimited, true)]
    [InlineData(ErrorCodes.TargetOffline, false)]
    [InlineData(ErrorCodes.NotAllowed, false)]
    [InlineData(ErrorCodes.Busy, false)]
    [InlineData("", false)]
    [InlineData(null, false)]
    public void ErrorCodes_IsRelayOnly(string? code, bool expected)
    {
        Assert.Equal(expected, ErrorCodes.IsRelayOnly(code));
    }
}
