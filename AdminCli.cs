/// <summary>The relay's admin command line (`reset-account`), kept free of the host so it can be tested directly.</summary>
internal static class AdminCli
{
    public const string ResetAccountCommand = "reset-account";

    public const string Usage =
        "Usage: dotnet WindUpRelay.dll [host options]\n"
        + "       dotnet WindUpRelay.dll reset-account <PAIRINGKEY> [host options]";

    /// <summary>The spellings of the command, each also accepted as `&lt;spelling&gt;=&lt;PAIRINGKEY&gt;`.</summary>
    private static readonly string[] CommandSpellings =
        [ResetAccountCommand, "--" + ResetAccountCommand, "-" + ResetAccountCommand, "/" + ResetAccountCommand];

    /// <summary>
    /// Finds the reset command anywhere in <paramref name="args"/> (so `--environment Production` may come first),
    /// written as `reset-account`, `--reset-account`, `-reset-account` or `/reset-account` (any case), with the key
    /// as the next non-blank argument or after `=`. Strips it with its key; <paramref name="rest"/> is everything
    /// else, for the host builder's configuration. False when the command isn't there (<paramref name="rest"/> is
    /// then <paramref name="args"/>). Blank (empty or whitespace-only) arguments are ignored throughout.
    /// <para>
    /// When it is there, either <paramref name="key"/> is the normalized pairing key, or <paramref name="error"/>
    /// says why the command can't run (the command given twice, a bare `--`, no key, an invalid key, or an argument
    /// besides the key that the host can't place). When it isn't, <paramref name="error"/> is set if the host's
    /// arguments have one it can't place: a positional argument (the server takes none) or an unknown option whose
    /// name contains "reset", either most likely a mistyped command. Whenever <paramref name="error"/> is set, the
    /// caller exits with code 2 without starting the host.
    /// </para>
    /// </summary>
    public static bool TryTakeResetAccount(string[] args, out string? key, out string[] rest, out string? error)
    {
        key = null;
        error = null;
        string? inlineKey = null;
        var index = Array.FindIndex(args, a => IsCommand(a, out inlineKey));
        if (index < 0)
        {
            rest = args;
            error = UnplacedArgument(args);
            return false;
        }

        // `reset-account KEY`: the next non-blank argument, unless it is an option (then the key is missing).
        var rawKey = inlineKey;
        var end = index + 1;
        if (rawKey is null)
        {
            var next = end;
            while (next < args.Length && IsBlank(args[next]))
                next++;
            if (next < args.Length && !IsOption(args[next]))
            {
                rawKey = args[next];
                end = next + 1;
            }
        }

        rest = [.. args[..index], .. args[end..]];

        if (Array.FindIndex(args, index + 1, a => IsCommand(a, out _)) >= 0)
        {
            error = "The reset-account command is given more than once.";
            return true;
        }

        // The host reads a bare `--` as an empty name that takes the next argument as its value, so whatever
        // follows it would be silently swallowed.
        if (Array.IndexOf(args, "--") >= 0)
        {
            error = "A bare -- can't be used with reset-account.";
            return true;
        }

        if (string.IsNullOrWhiteSpace(rawKey))
        {
            error = "Missing pairing key.";
            return true;
        }

        if (!TryNormalizeKey(rawKey, out var normalized))
        {
            error = "Invalid pairing key: expected 8 letters or digits.";
            return true;
        }

        if (UnplacedArgument(rest) is { } unplaced)
        {
            error = unplaced;
            return true;
        }

        key = normalized;
        return true;
    }

    private static bool IsCommand(string arg, out string? inlineKey)
    {
        foreach (var spelling in CommandSpellings)
        {
            if (string.Equals(arg, spelling, StringComparison.OrdinalIgnoreCase))
            {
                inlineKey = null;
                return true;
            }

            if (arg.Length > spelling.Length
                && arg[spelling.Length] == '='
                && arg.StartsWith(spelling, StringComparison.OrdinalIgnoreCase))
            {
                inlineKey = arg[(spelling.Length + 1)..];
                return true;
            }
        }

        inlineKey = null;
        return false;
    }

    private static bool IsOption(string arg) => arg.StartsWith('-') || arg.StartsWith('/');

    private static bool IsBlank(string arg) => string.IsNullOrWhiteSpace(arg);

    /// <summary>
    /// Normalizes a pairing key given on the command line. It may contain only letters, digits, `-` and spaces (so
    /// `Mode=Bind`, `a:b` or `/x.db` are never taken for one) and must normalize to 8 letters or digits.
    /// </summary>
    private static bool TryNormalizeKey(string rawKey, out string key)
    {
        key = SessionHub.NormalizePairingKey(rawKey);
        return key.Length == 8 && rawKey.All(c => char.IsAsciiLetterOrDigit(c) || c is '-' or ' ');
    }

    /// <summary>
    /// The name of an option (`--name`, `-name` or `/name`, each optionally with `=value`) or of a `name=value`
    /// token: the part before any `=`, without the prefix. Null for a positional argument.
    /// </summary>
    private static string? OptionName(string arg)
    {
        var name = arg.StartsWith("--", StringComparison.Ordinal) ? arg[2..] : IsOption(arg) ? arg[1..] : arg;
        var separator = name.IndexOf('=');
        if (separator >= 0)
            return name[..separator];
        return IsOption(arg) ? name : null;
    }

    /// <summary>
    /// Why the host can't place an argument in <paramref name="args"/>, or null: the first positional argument (one
    /// that isn't an option or an option's value) or the first unknown option whose name contains "reset". Read the
    /// way the host's command-line configuration reads them: `name=value`, `--name=value` and `/name=value` stand
    /// alone; `--name value` and `/name value` take the next argument as the value (never checked, so
    /// `--contentRoot /srv/reset` is fine); a lone `-x` takes none; blank arguments are skipped. A bare `--` is read
    /// as taking no value, unlike the host (which reads it as an empty name and takes the next argument as its
    /// value), so `-- extra` is still refused on a server start; a reset command refuses a bare `--` outright.
    /// </summary>
    private static string? UnplacedArgument(string[] args)
    {
        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            if (IsBlank(arg) || arg == "--")
                continue;
            if (OptionName(arg) is { } name
                && name.Contains("reset", StringComparison.OrdinalIgnoreCase)
                && !IsCommand(arg, out _))
                return $"Unknown option: {arg}";
            if (arg.Contains('='))
                continue;
            if (arg.StartsWith("--", StringComparison.Ordinal) || arg.StartsWith('/'))
            {
                i++;
                continue;
            }

            if (!arg.StartsWith('-'))
                return $"Unexpected argument: {arg}";
        }

        return null;
    }

    /// <summary>
    /// Deletes the account for <paramref name="rawKey"/> in the database at <paramref name="dbPath"/>, so the next
    /// authenticated register claims the key again. Returns the exit code: 0 deleted, 1 no database or no account
    /// (nothing changed), 2 invalid key. Prints the full database path before acting. Never creates the database.
    /// </summary>
    public static int ResetAccount(string dbPath, string rawKey, TextWriter output, TextWriter error)
    {
        if (!TryNormalizeKey(rawKey, out var key))
        {
            error.WriteLine("Invalid pairing key: expected 8 letters or digits.");
            return 2;
        }

        // Relative paths resolve against the current directory, as the server's do; show where that lands.
        dbPath = Path.GetFullPath(dbPath);
        output.WriteLine($"Database: {dbPath}");

        if (!File.Exists(dbPath))
        {
            error.WriteLine($"Database not found: {dbPath}");
            return 1;
        }

        using var store = new RelayStore(dbPath);
        if (!store.DeleteAccount(key))
        {
            output.WriteLine($"No relay account for {key} in {dbPath}; nothing changed.");
            return 1;
        }

        store.RecordEvent("account_reset", pairingKey: key);
        output.WriteLine($"Deleted relay account for {key} in {dbPath}. The next register with a secret claims it again.");
        return 0;
    }
}
