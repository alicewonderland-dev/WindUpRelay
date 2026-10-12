namespace WindUpRelay.Tests;

public class RelayTokenAuthTests
{
    private const string Token = "s3cret-relay-token";

    [Theory]
    [InlineData("Bearer " + Token, Token)]
    [InlineData("bearer " + Token, Token)]
    [InlineData("Bearer   " + Token + "  ", Token)]
    public void ParseBearer_ReadsTheCredential(string header, string expected)
    {
        Assert.Equal(expected, RelayTokenAuth.ParseBearer(header));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("Bearer ")]
    [InlineData("Basic " + Token)]
    [InlineData(Token)]
    [InlineData("xBearer " + Token)]
    public void ParseBearer_RejectsOtherHeaders(string? header)
    {
        Assert.Null(RelayTokenAuth.ParseBearer(header));
    }

    [Fact]
    public void ParseBearer_StripsOnlyThePrefix()
    {
        // The old Replace("Bearer ", "") also removed the text from inside the credential.
        Assert.Equal("abcBearer def", RelayTokenAuth.ParseBearer("Bearer abcBearer def"));
        Assert.False(RelayTokenAuth.IsAuthorized(
            RelayTokenAuth.GetProvidedToken(null, "Bearer s3cret-Bearer relay-token"), Token));
    }

    [Fact]
    public void QueryToken_IsStillAccepted_AndWins()
    {
        Assert.Equal(Token, RelayTokenAuth.GetProvidedToken(Token, null));
        Assert.Equal(Token, RelayTokenAuth.GetProvidedToken(null, "Bearer " + Token));
        Assert.Equal("wrong", RelayTokenAuth.GetProvidedToken("wrong", "Bearer " + Token));
    }

    [Fact]
    public void IsAuthorized_ComparesExactly()
    {
        Assert.True(RelayTokenAuth.IsAuthorized(Token, Token));
        Assert.False(RelayTokenAuth.IsAuthorized(null, Token));
        Assert.False(RelayTokenAuth.IsAuthorized("", Token));
        Assert.False(RelayTokenAuth.IsAuthorized(Token + "x", Token));
        Assert.False(RelayTokenAuth.IsAuthorized(Token.ToUpperInvariant(), Token));
    }

    [Fact]
    public void IsAuthorized_NoConfiguredToken_AllowsEverything()
    {
        Assert.True(RelayTokenAuth.IsAuthorized(null, ""));
        Assert.True(RelayTokenAuth.IsAuthorized("anything", ""));
    }
}
