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

    /// <summary>
    /// 05-D2: a valid secret for an unclaimed key, but this client address created too many accounts recently. No account
    /// is created and no session mapped (never a legacy fallback): close with 1013 <c>bind_rate_limited</c>. Also the
    /// re-decision after a lost race that would otherwise bind with no account (05-D10: the same refusal and close,
    /// recorded as <c>bind_race</c>).
    /// </summary>
    TryLater,
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
    /// <para>
    /// 05-D2: only a Bound outcome asks <paramref name="tryReserveFirstBind"/> (the per-address limit) for a slot, just
    /// before the insert; every other outcome never consumes one. Refused, it's <see cref="RegisterAuthOutcome.TryLater"/>
    /// and nothing is inserted. A slot whose insert lost the race, or threw (05-D11), goes back through
    /// <paramref name="refundFirstBind"/>. The re-decision after a lost race can never come out Bound (that would be a
    /// bound session with no account, e.g. if the winner's account was reset in between): that is TryLater too, and the
    /// client simply registers again. 05-D10: that refusal is reported through <paramref name="onBindRace"/>, so it's
    /// recorded as <c>bind_race</c>, not as the limit.
    /// </para>
    /// </summary>
    public static RegisterAuthOutcome Resolve(
        RelayAuthMode mode,
        string? providedSecret,
        Func<string?> readStoredHash,
        Func<string, bool> tryCreateAccount,
        Func<bool>? tryReserveFirstBind = null,
        Action? refundFirstBind = null,
        Action? onBindRace = null)
    {
        if (mode == RelayAuthMode.Off)
            return RegisterAuthOutcome.Legacy;

        var outcome = Decide(mode, readStoredHash(), providedSecret);
        if (outcome != RegisterAuthOutcome.Bound)
            return outcome;

        if (tryReserveFirstBind is not null && !tryReserveFirstBind())
            return RegisterAuthOutcome.TryLater;

        bool created;
        try
        {
            created = tryCreateAccount(RelaySecret.HashHex(providedSecret)!);
        }
        catch
        {
            // 05-D11: no account was created, so the slot goes back; the failure itself is the caller's.
            refundFirstBind?.Invoke();
            throw;
        }

        if (created)
            return RegisterAuthOutcome.Bound;

        refundFirstBind?.Invoke();
        outcome = Decide(mode, readStoredHash(), providedSecret);
        if (outcome != RegisterAuthOutcome.Bound)
            return outcome;

        onBindRace?.Invoke();
        return RegisterAuthOutcome.TryLater;
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
