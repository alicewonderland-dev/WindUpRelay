using System.Security.Cryptography;
using System.Text;

/// <summary>Shared relay token check for <c>/ws</c> and <c>/stats</c>.</summary>
internal static class RelayTokenAuth
{
    private const string BearerPrefix = "Bearer ";

    /// <summary>
    /// The token from <c>?token=</c> (old clients) or, failing that, an <c>Authorization: Bearer</c> header.
    /// Null when neither is present.
    /// </summary>
    public static string? GetProvidedToken(string? queryToken, string? authorizationHeader)
        => queryToken ?? ParseBearer(authorizationHeader);

    /// <summary>The credential of an <c>Authorization: Bearer &lt;token&gt;</c> header, or null for any other scheme.</summary>
    public static string? ParseBearer(string? authorizationHeader)
    {
        if (authorizationHeader is null
            || !authorizationHeader.StartsWith(BearerPrefix, StringComparison.OrdinalIgnoreCase))
            return null;

        var token = authorizationHeader[BearerPrefix.Length..].Trim();
        return token.Length == 0 ? null : token;
    }

    /// <summary>
    /// Constant-time comparison. Both sides are hashed first so the comparison doesn't leak the token length.
    /// An empty <paramref name="expected"/> means no token is configured (local dev) and allows everything.
    /// </summary>
    public static bool IsAuthorized(string? provided, string expected)
    {
        if (string.IsNullOrEmpty(expected))
            return true;
        if (provided is null)
            return false;

        var providedHash = SHA256.HashData(Encoding.UTF8.GetBytes(provided));
        var expectedHash = SHA256.HashData(Encoding.UTF8.GetBytes(expected));
        return CryptographicOperations.FixedTimeEquals(providedHash, expectedHash);
    }
}
