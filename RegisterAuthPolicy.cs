using WindUpKey.Protocol;

/// <summary>Relay:AuthMode. See README for the rollout order.</summary>
internal enum RelayAuthMode
{
    /// <summary>Pre-auth behaviour: secrets are ignored (rollback).</summary>
    Off,

    /// <summary>Trust on first use; secretless registers allowed only for unclaimed keys.</summary>
    Bind,

    /// <summary>Like <see cref="Bind"/>, but every register must carry a secret.</summary>
    Enforce,
}

internal enum RegisterAuthOutcome
{
    /// <summary>Accepted without authentication (today's behaviour, incl. pair_key_collision).</summary>
    Legacy,

    /// <summary>Valid secret for an unclaimed key: create the account (insert-if-absent, then re-read).</summary>
    Bound,

    /// <summary>Secret matches the stored hash.</summary>
    Authenticated,

    /// <summary>Secret sent but wrong or malformed: send auth_failed, then close.</summary>
    AuthFailed,

    /// <summary>No secret where one is required: close without an error envelope (old clients would spam chat).</summary>
    RefuseSilently,
}

internal static class RegisterAuthPolicy
{
    /// <summary>
    /// Pure register decision. <paramref name="storedHash"/> is the account's secret hash, or null when the
    /// key has no account; <paramref name="providedSecret"/> is the raw register secret (null/empty when absent).
    /// </summary>
    public static RegisterAuthOutcome Decide(RelayAuthMode mode, string? storedHash, string? providedSecret)
    {
        if (mode == RelayAuthMode.Off)
            return RegisterAuthOutcome.Legacy;

        if (string.IsNullOrEmpty(providedSecret))
        {
            if (mode == RelayAuthMode.Enforce || storedHash is not null)
                return RegisterAuthOutcome.RefuseSilently;
            return RegisterAuthOutcome.Legacy;
        }

        var providedHash = RelaySecret.HashHex(providedSecret);
        if (providedHash is null)
            return RegisterAuthOutcome.AuthFailed;

        if (storedHash is null)
            return RegisterAuthOutcome.Bound;

        return RelaySecret.HashEquals(storedHash, providedHash)
            ? RegisterAuthOutcome.Authenticated
            : RegisterAuthOutcome.AuthFailed;
    }

    /// <summary>
    /// <see cref="Decide"/> plus account creation. A Bound outcome inserts the account if absent; when a
    /// concurrent first bind won, the winner's hash is re-read and the register is decided again.
    /// Off never touches the store.
    /// </summary>
    public static RegisterAuthOutcome Resolve(
        RelayAuthMode mode,
        string? providedSecret,
        Func<string?> readStoredHash,
        Func<string, bool> tryCreateAccount)
    {
        if (mode == RelayAuthMode.Off)
            return RegisterAuthOutcome.Legacy;

        var outcome = Decide(mode, readStoredHash(), providedSecret);
        if (outcome == RegisterAuthOutcome.Bound && !tryCreateAccount(RelaySecret.HashHex(providedSecret)!))
            outcome = Decide(mode, readStoredHash(), providedSecret);
        return outcome;
    }

    /// <summary>Parses Relay:AuthMode. Blank means <see cref="RelayAuthMode.Bind"/>; anything unknown is rejected.</summary>
    public static bool TryParseMode(string? raw, out RelayAuthMode mode)
    {
        mode = RelayAuthMode.Bind;
        if (string.IsNullOrWhiteSpace(raw))
            return true;

        switch (raw.Trim().ToLowerInvariant())
        {
            case "off":
                mode = RelayAuthMode.Off;
                return true;
            case "bind":
                mode = RelayAuthMode.Bind;
                return true;
            case "enforce":
                mode = RelayAuthMode.Enforce;
                return true;
            default:
                return false;
        }
    }
}
