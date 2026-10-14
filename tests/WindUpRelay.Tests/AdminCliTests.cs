using Microsoft.Data.Sqlite;
using WindUpKey.Protocol;

namespace WindUpRelay.Tests;

public class AdminCliTests
{
    private static string[] Split(string commandLine) => commandLine.Split(' ');

    [Fact]
    public void NoCommand_LeavesTheArgumentsForTheHost()
    {
        string[] args = ["--environment", "Production"];
        Assert.False(AdminCli.TryTakeResetAccount(args, out var key, out var rest, out var error));
        Assert.Null(key);
        Assert.Null(error);
        Assert.Equal(args, rest);
        Assert.False(AdminCli.TryTakeResetAccount([], out _, out rest, out error));
        Assert.Empty(rest);
        Assert.Null(error);
        // Other words that merely contain the command aren't it (and are refused below).
        Assert.False(AdminCli.TryTakeResetAccount(["reset-accounts", "ABCD1234"], out _, out _, out _));
        Assert.False(AdminCli.TryTakeResetAccount(["--reset-accounts=ABCD1234"], out _, out _, out _));
    }

    /// <summary>
    /// Server starts with options are unchanged: nothing to refuse, everything goes to the host. 08-D1: a bare `--`
    /// (or `/`) takes the next argument, as the host reads it, so `-- extra` is accepted again.
    /// </summary>
    [Theory]
    [InlineData("--environment Production")]
    [InlineData("--environment Development Relay:AuthMode=Bind")]
    [InlineData("--urls http://127.0.0.1:29417 --environment Production")]
    [InlineData("/environment Production --Relay:AuthMode Enforce")]
    [InlineData("--Relay:DatabasePath=/x.db /Relay:Token=x Relay:AuthMode=Off")]
    [InlineData("-x --environment Production")]
    [InlineData("--environment Production --")]
    [InlineData("-- extra")]
    [InlineData("/ extra")]
    [InlineData("--environment Production -- extra")]
    [InlineData("--environment Production --Relay:Token")]
    [InlineData("--contentRoot /srv/reset-relay --urls http://127.0.0.1:29417")]
    [InlineData("/contentRoot /srv/reset --Relay:DatabasePath=/var/reset/relay.db Relay:Token=reset")]
    public void ServerStart_WithOptions_IsNotRefused(string commandLine)
    {
        var args = Split(commandLine);
        Assert.False(AdminCli.TryTakeResetAccount(args, out var key, out var rest, out var error));
        Assert.Null(key);
        Assert.Null(error);
        Assert.Equal(args, rest);
    }

    /// <summary>
    /// The server takes no positional arguments, so a mistyped command can't start it. 08-D1: `-- --environment
    /// Production` leaves `Production` positional (the host would have started in Development).
    /// </summary>
    [Theory]
    [InlineData("reset-acount ABCD1234")]
    [InlineData("resetaccount ABCD1234")]
    [InlineData("reset-accounts ABCD1234")]
    [InlineData("--environment Production reset-acount ABCD1234")]
    [InlineData("--environment Production extra")]
    [InlineData("Production")]
    [InlineData("-x extra")]
    [InlineData("-- --environment Production")]
    [InlineData("/ --environment Production")]
    public void PositionalArgument_WithoutTheCommand_IsRefused(string commandLine)
    {
        var args = Split(commandLine);
        Assert.False(AdminCli.TryTakeResetAccount(args, out var key, out var rest, out var error));
        Assert.Null(key);
        Assert.Contains("Unexpected argument", error);
        Assert.Equal(args, rest);
    }

    /// <summary>Decision 42: a mistyped command written as an option or `name=value` can't start the server either.</summary>
    [Theory]
    [InlineData("--reset-acount ABCD1234")]
    [InlineData("/resetaccount ABCD1234")]
    [InlineData("reset-acount=ABCD1234")]
    [InlineData("--reset-account-ABCD1234")]
    [InlineData("-reset-acount ABCD1234")]
    [InlineData("--reset-acount=ABCD1234")]
    [InlineData("/RESETACCOUNT=ABCD1234")]
    [InlineData("--reset-accounts=ABCD1234")]
    [InlineData("--environment Production --Reset-Acount ABCD1234")]
    [InlineData("--environment Production Relay:ResetKey=1")]
    [InlineData("-x --reset")]
    public void UnknownResetOption_WithoutTheCommand_IsRefused(string commandLine)
    {
        var args = Split(commandLine);
        Assert.False(AdminCli.TryTakeResetAccount(args, out var key, out var rest, out var error));
        Assert.Null(key);
        Assert.Contains("Unknown option", error);
        Assert.Equal(args, rest);
    }

    [Theory]
    [InlineData("reset-account ABCD1234 --reset-acount WXYZ9876")]
    [InlineData("--force-reset reset-account ABCD1234")]
    [InlineData("reset-account=ABCD1234 /reset-account-WXYZ9876")]
    [InlineData("reset-account ABCD1234 resetaccount=WXYZ9876")]
    public void UnknownResetOption_WithTheCommand_IsAnError(string commandLine)
    {
        Assert.True(AdminCli.TryTakeResetAccount(Split(commandLine), out var key, out _, out var error));
        Assert.Null(key);
        Assert.Contains("Unknown option", error);
    }

    /// <summary>Decision 43: blank arguments are ignored everywhere.</summary>
    [Fact]
    public void BlankArguments_AreIgnored()
    {
        string[] server = ["", "--environment", "Production", " ", "\t"];
        Assert.False(AdminCli.TryTakeResetAccount(server, out var key, out var rest, out var error));
        Assert.Null(error);
        Assert.Equal(server, rest);

        Assert.True(AdminCli.TryTakeResetAccount(["", "reset-account", " ", "", "abcd-1234", "\t"], out key, out _, out error));
        Assert.Null(error);
        Assert.Equal("ABCD1234", key);

        Assert.True(AdminCli.TryTakeResetAccount(
            [" ", "--reset-account", "", "ABCD1234", "--environment", "Production", ""], out key, out rest, out error));
        Assert.Null(error);
        Assert.Equal("ABCD1234", key);
        Assert.Equal(new[] { "--environment", "Production" }, rest.Where(a => a.Trim().Length > 0));

        Assert.True(AdminCli.TryTakeResetAccount(["reset-account", " ", ""], out key, out _, out error));
        Assert.Null(key);
        Assert.Contains("Missing pairing key", error);
        Assert.True(AdminCli.TryTakeResetAccount(["reset-account= "], out key, out _, out error));
        Assert.Null(key);
        Assert.Contains("Missing pairing key", error);
    }

    [Theory]
    [InlineData("reset-account ABCD1234 --environment Production")]
    [InlineData("--environment Production reset-account ABCD1234")]
    [InlineData("--environment Production Reset-Account ABCD1234")]
    [InlineData("--reset-account ABCD1234 --environment Production")]
    [InlineData("-reset-account ABCD1234 --environment Production")]
    [InlineData("--environment Production -Reset-Account ABCD1234")]
    [InlineData("/reset-account ABCD1234 --environment Production")]
    [InlineData("--environment Production /RESET-ACCOUNT ABCD1234")]
    [InlineData("reset-account=ABCD1234 --environment Production")]
    [InlineData("--environment Production --reset-account=ABCD1234")]
    [InlineData("--environment Production /reset-account=ABCD1234")]
    [InlineData("-reset-account=ABCD1234 --environment Production")]
    public void Command_IsFoundAnywhere_InEveryForm_AndOnlyItAndItsKeyAreStripped(string commandLine)
    {
        Assert.True(AdminCli.TryTakeResetAccount(Split(commandLine), out var key, out var rest, out var error));
        Assert.Null(error);
        Assert.Equal("ABCD1234", key);
        Assert.Equal(new[] { "--environment", "Production" }, rest);
    }

    [Fact]
    public void OtherArguments_KeepTheirOrder_AndTheKeyIsNormalized()
    {
        Assert.True(AdminCli.TryTakeResetAccount(
            ["--Relay:DatabasePath=/x.db", "reset-account", "abcd-1234", "--environment", "Production"], out var key, out var rest, out var error));
        Assert.Null(error);
        Assert.Equal("ABCD1234", key);
        Assert.Equal(new[] { "--Relay:DatabasePath=/x.db", "--environment", "Production" }, rest);
    }

    /// <summary>Options and their values after the key, read the way the host's command-line configuration reads them.</summary>
    [Theory]
    [InlineData("reset-account ABCD1234 --Relay:DatabasePath=/x.db")]
    [InlineData("reset-account ABCD1234 --Relay:DatabasePath /x.db")]
    [InlineData("reset-account ABCD1234 /Relay:DatabasePath /x.db")]
    [InlineData("reset-account ABCD1234 /Relay:DatabasePath=/x.db")]
    [InlineData("reset-account ABCD1234 Relay:DatabasePath=/x.db")]
    [InlineData("reset-account=ABCD1234 --environment Production --Relay:Token x")]
    [InlineData("reset-account ABCD1234 --contentRoot /srv/reset-relay")]
    public void OptionsAfterTheKey_AreAllowed(string commandLine)
    {
        Assert.True(AdminCli.TryTakeResetAccount(Split(commandLine), out var key, out _, out var error));
        Assert.Null(error);
        Assert.Equal("ABCD1234", key);
    }

    [Theory]
    [InlineData("reset-account ABCD1234 extra")]
    [InlineData("reset-account ABCD1234 WXYZ9876")]
    [InlineData("reset-account ABCD1234 --environment Production extra")]
    [InlineData("reset-account=ABCD1234 extra")]
    [InlineData("/reset-account ABCD1234 -x extra")]
    [InlineData("extra reset-account ABCD1234")]
    [InlineData("--environment Production extra -reset-account=ABCD1234")]
    public void PositionalArgumentBesidesTheKey_IsRejected(string commandLine)
    {
        Assert.True(AdminCli.TryTakeResetAccount(Split(commandLine), out var key, out _, out var error));
        Assert.Null(key);
        Assert.Contains("Unexpected argument", error);
    }

    /// <summary>Decision 41: the host would read a bare `--` as an empty name swallowing the next argument.</summary>
    [Theory]
    [InlineData("reset-account ABCD1234 /")]
    [InlineData("reset-account ABCD1234 / --Relay:DatabasePath=/x.db")]
    [InlineData("/ reset-account ABCD1234")]
    [InlineData("reset-account ABCD1234 --")]
    [InlineData("reset-account ABCD1234 -- --Relay:DatabasePath=/x.db")]
    [InlineData("reset-account ABCD1234 -- WXYZ9876")]
    [InlineData("-- reset-account ABCD1234")]
    [InlineData("reset-account -- ABCD1234")]
    [InlineData("reset-account=ABCD1234 --environment Production --")]
    public void BareDoubleDash_WithTheCommand_IsAnError(string commandLine)
    {
        Assert.True(AdminCli.TryTakeResetAccount(Split(commandLine), out var key, out _, out var error));
        Assert.Null(key);
        Assert.Contains("bare --", error);
    }

    /// <summary>08-C1.1: a blank option name (`"-- "`, `"/ "`) swallows the next argument just like a bare `--`.</summary>
    [Theory]
    [InlineData("-- ")]
    [InlineData("/ ")]
    [InlineData("--\t")]
    [InlineData("/")]
    public void NamelessOption_WithTheCommand_IsAnError(string nameless)
    {
        Assert.True(AdminCli.TryTakeResetAccount(
            ["reset-account", "ABCD1234", nameless, "--Relay:DatabasePath=/x.db"], out var key, out _, out var error));
        Assert.Null(key);
        Assert.StartsWith("A bare --", error);
        Assert.True(AdminCli.TryTakeResetAccount([nameless, "reset-account", "ABCD1234"], out key, out _, out error));
        Assert.Null(key);
        Assert.StartsWith("A bare --", error);
    }

    /// <summary>
    /// 08-C1.1: only `--` and `/` with a blank name are nameless. A single `-` is ignored by the host, and `--=x` or
    /// `/=x` take no value.
    /// </summary>
    [Theory]
    [InlineData("reset-account ABCD1234 -")]
    [InlineData("reset-account ABCD1234 - --environment Production")]
    [InlineData("reset-account ABCD1234 --=x")]
    [InlineData("reset-account ABCD1234 /=x")]
    public void OtherBlankishOptions_WithTheCommand_AreAllowed(string commandLine)
    {
        Assert.True(AdminCli.TryTakeResetAccount(Split(commandLine), out var key, out _, out var error));
        Assert.Null(error);
        Assert.Equal("ABCD1234", key);
    }

    /// <summary>08-C1.2 (D3): the host throws on a single-dash `-x=y`, so it's refused with or without the command.</summary>
    [Theory]
    [InlineData("-x=y")]
    [InlineData("-=x")]
    [InlineData("--environment Production -x=y")]
    [InlineData("-Relay:Token=x --environment Production")]
    public void SingleDashWithAValue_IsAnUnknownOption(string commandLine)
    {
        var args = Split(commandLine);
        Assert.False(AdminCli.TryTakeResetAccount(args, out var key, out var rest, out var error));
        Assert.Null(key);
        Assert.StartsWith("Unknown option: -", error);
        Assert.Equal(args, rest);

        Assert.True(AdminCli.TryTakeResetAccount(["reset-account", "ABCD1234", .. args], out key, out _, out error));
        Assert.Null(key);
        Assert.StartsWith("Unknown option: -", error);
        Assert.True(AdminCli.TryTakeResetAccount([.. args, "--reset-account=ABCD1234"], out key, out _, out error));
        Assert.Null(key);
        Assert.StartsWith("Unknown option: -", error);
    }

    [Theory]
    [InlineData("reset-account ABCD1234 --reset-account WXYZ9876")]
    [InlineData("--reset-account=ABCD1234 --reset-account=WXYZ9876")]
    [InlineData("reset-account ABCD1234 -reset-account WXYZ9876")]
    [InlineData("/reset-account=ABCD1234 --environment Production RESET-ACCOUNT=WXYZ9876")]
    [InlineData("--reset-account ABCD1234 reset-account")]
    [InlineData("reset-account reset-account ABCD1234")]
    [InlineData("reset-account ABCD1234 --Relay:Token reset-account")]
    public void Command_GivenTwice_IsAnError(string commandLine)
    {
        Assert.True(AdminCli.TryTakeResetAccount(Split(commandLine), out var key, out _, out var error));
        Assert.Null(key);
        Assert.Contains("more than once", error);
    }

    [Theory]
    [InlineData("reset-account")]
    [InlineData("--environment Production reset-account")]
    [InlineData("reset-account --environment Production")]
    [InlineData("--reset-account")]
    [InlineData("-reset-account")]
    [InlineData("/reset-account")]
    [InlineData("reset-account=")]
    [InlineData("--reset-account= --environment Production")]
    [InlineData("-reset-account= --environment Production")]
    [InlineData("/reset-account=")]
    public void Command_WithoutAKey_IsAnError(string commandLine)
    {
        Assert.True(AdminCli.TryTakeResetAccount(Split(commandLine), out var key, out var rest, out var error));
        Assert.Null(key);
        Assert.Contains("Missing pairing key", error);
        Assert.DoesNotContain(rest, a => a.Contains("reset-account", StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData("reset-account bad")]
    [InlineData("--reset-account ABCD12345")]
    [InlineData("/reset-account ABC-123")]
    [InlineData("reset-account=bad")]
    [InlineData("--reset-account=ABCD12345 --environment Production")]
    [InlineData("-reset-account bad")]
    [InlineData("reset-account Mode=Bind")]
    [InlineData("reset-account=ABCD=1234")]
    [InlineData("reset-account ABCD:1234")]
    [InlineData("--reset-account=ABCD:1234")]
    [InlineData("reset-account ABCD/1234")]
    [InlineData("/reset-account=AB/CD1234")]
    [InlineData("reset-account ABCD_1234")]
    [InlineData("reset-account Relay:AuthMode=Bind ABCD1234")]
    public void Command_WithAnInvalidKey_IsAnError(string commandLine)
    {
        Assert.True(AdminCli.TryTakeResetAccount(Split(commandLine), out var key, out _, out var error));
        Assert.Null(key);
        Assert.Contains("Invalid pairing key", error);
    }

    [Fact]
    public void Key_MayContainDashesAndSpaces()
    {
        Assert.True(AdminCli.TryTakeResetAccount(["reset-account", " abcd-12 34 "], out var key, out _, out var error));
        Assert.Null(error);
        Assert.Equal("ABCD1234", key);
        Assert.True(AdminCli.TryTakeResetAccount(["--reset-account=AB CD-1234"], out key, out _, out error));
        Assert.Null(error);
        Assert.Equal("ABCD1234", key);
    }
}

/// <summary>
/// The relay's entry point with admin command lines: bad ones (including a server start with a positional argument)
/// exit 2 before the web host is built and reset nothing, and good ones run the reset without starting it. The entry point sets the process exit code, so these run one at a time and
/// put it back.
/// </summary>
public sealed class AdminCliEntryPointTests : IDisposable
{
    private static readonly TimeSpan HostStartTimeout = TimeSpan.FromSeconds(20);
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "windup-relay-cli-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        Environment.ExitCode = 0;
        SqliteConnection.ClearAllPools();
        try
        {
            Directory.Delete(_dir, recursive: true);
        }
        catch (IOException)
        {
            // Best effort temp cleanup.
        }
    }

    private string _stdout = string.Empty;
    private string _stderr = string.Empty;

    /// <summary>
    /// Runs the relay's Main, capturing what it writes to stdout and stderr. A started host never returns, which
    /// fails the test instead of hanging it.
    /// </summary>
    private int RunMain(params string[] args)
    {
        var entry = typeof(AdminCli).Assembly.EntryPoint!;
        Environment.ExitCode = 0;
        var (stdout, stderr) = (new StringWriter(), new StringWriter());
        var (oldOut, oldErr) = (Console.Out, Console.Error);
        Console.SetOut(stdout);
        Console.SetError(stderr);
        try
        {
            var run = Task.Run(() => entry.Invoke(null, [args]));
            Assert.True(run.Wait(HostStartTimeout), "the relay didn't return: it started the web host");
        }
        finally
        {
            Console.SetOut(oldOut);
            Console.SetError(oldErr);
        }

        (_stdout, _stderr) = (stdout.ToString(), stderr.ToString());
        return Environment.ExitCode;
    }

    [Theory]
    [InlineData("reset-account")]
    [InlineData("--reset-account")]
    [InlineData("-reset-account")]
    [InlineData("/reset-account")]
    [InlineData("reset-account=")]
    [InlineData("reset-account bad")]
    [InlineData("--reset-account bad")]
    [InlineData("-reset-account bad")]
    [InlineData("/reset-account bad")]
    [InlineData("reset-account=bad")]
    [InlineData("--environment Development reset-account")]
    [InlineData("reset-account ABCD1234 extra")]
    [InlineData("--reset-account=ABCD1234 extra --environment Development")]
    [InlineData("reset-acount ABCD1234")]
    [InlineData("resetaccount ABCD1234")]
    [InlineData("--environment Development reset-acount ABCD1234")]
    [InlineData("--environment Development Relay:AuthMode=Bind extra")]
    [InlineData("reset-account ABCD1234 --reset-account WXYZ9876")]
    [InlineData("--reset-account=ABCD1234 --reset-account=WXYZ9876")]
    [InlineData("reset-account ABCD1234 -- WXYZ9876")]
    [InlineData("-reset-account ABCD1234 /reset-account=WXYZ9876")]
    [InlineData("reset-account Mode=Bind")]
    [InlineData("reset-account=ABCD=1234")]
    [InlineData("reset-account ABCD:1234")]
    [InlineData("reset-account WXYZ/9876")]
    [InlineData("reset-account ABCD1234 --")]
    [InlineData("-- reset-account ABCD1234")]
    [InlineData("--reset-acount ABCD1234")]
    [InlineData("/resetaccount ABCD1234")]
    [InlineData("reset-acount=ABCD1234")]
    [InlineData("--reset-account-ABCD1234")]
    [InlineData("-reset-acount ABCD1234")]
    [InlineData("--environment Development --Reset-Acount=ABCD1234")]
    [InlineData("reset-account ABCD1234 --reset-acount WXYZ9876")]
    [InlineData("reset-account ABCD1234 /")]
    [InlineData("reset-account ABCD1234 -x=y")]
    [InlineData("--environment Development -x=y")]
    public void BadCommandLines_Exit2_WithoutStartingTheHost_AndResetNothing(string commandLine)
    {
        Directory.CreateDirectory(_dir);
        var db = Path.Combine(_dir, "relay.db");
        using (var store = new RelayStore(db))
        {
            store.TryCreateAccount("ABCD1234", RelaySecret.HashHex(RelaySecret.Generate())!);
            store.TryCreateAccount("WXYZ9876", RelaySecret.HashHex(RelaySecret.Generate())!);
            store.TryCreateAccount("MODEBIND", RelaySecret.HashHex(RelaySecret.Generate())!);
        }

        Assert.Equal(2, RunMain([.. Split(commandLine), $"--Relay:DatabasePath={db}"]));
        Assert.Empty(_stdout);
        Assert.Contains(AdminCli.Usage, _stderr);
        Assert.Matches("^(Unexpected argument|Unknown option|Invalid pairing key|Missing pairing key|A bare --|The reset-account command)", _stderr);
        using (var store = new RelayStore(db))
        {
            Assert.NotNull(store.GetAccountSecretHash("ABCD1234"));
            Assert.NotNull(store.GetAccountSecretHash("WXYZ9876"));
            Assert.NotNull(store.GetAccountSecretHash("MODEBIND"));
        }
    }

    [Theory]
    [InlineData("reset-account abcd1234")]
    [InlineData("--reset-account ABCD1234")]
    [InlineData("-reset-account ABCD1234")]
    [InlineData("/reset-account ABCD-1234")]
    [InlineData("reset-account=abcd1234")]
    [InlineData("-reset-account=abcd1234")]
    public void GoodCommandLines_ResetTheAccount_WithoutStartingTheHost(string commandLine)
    {
        Directory.CreateDirectory(_dir);
        var db = Path.Combine(_dir, "relay.db");
        using (var store = new RelayStore(db))
            store.TryCreateAccount("ABCD1234", RelaySecret.HashHex(RelaySecret.Generate())!);

        Assert.Equal(0, RunMain([.. Split(commandLine), $"--Relay:DatabasePath={db}"]));
        Assert.StartsWith($"Database: {db}{Environment.NewLine}", _stdout);
        Assert.Contains("Deleted relay account for ABCD1234", _stdout);
        Assert.Empty(_stderr);
        using (var store = new RelayStore(db))
            Assert.Null(store.GetAccountSecretHash("ABCD1234"));

        Assert.Equal(1, RunMain([$"--Relay:DatabasePath={db}", .. Split(commandLine)]));
        Assert.StartsWith($"Database: {db}{Environment.NewLine}", _stdout);
        Assert.Empty(_stderr);
    }

    /// <summary>Decision 43: blank arguments anywhere, even between the command and its key, are ignored.</summary>
    [Fact]
    public void BlankArguments_AreIgnored_ByTheEntryPoint()
    {
        Directory.CreateDirectory(_dir);
        var db = Path.Combine(_dir, "relay.db");
        using (var store = new RelayStore(db))
            store.TryCreateAccount("ABCD1234", RelaySecret.HashHex(RelaySecret.Generate())!);

        Assert.Equal(0, RunMain("", "reset-account", " ", "abcd1234", "", $"--Relay:DatabasePath={db}", "\t"));
        Assert.StartsWith($"Database: {db}{Environment.NewLine}", _stdout);
        using (var store = new RelayStore(db))
            Assert.Null(store.GetAccountSecretHash("ABCD1234"));
    }

    /// <summary>A missing database is reported on stderr, after the path on stdout.</summary>
    [Fact]
    public void MissingDatabase_GoesToStderr()
    {
        var db = Path.Combine(_dir, "missing.db");
        Assert.Equal(1, RunMain("reset-account", "ABCD1234", $"--Relay:DatabasePath={db}"));
        Assert.Equal($"Database: {db}{Environment.NewLine}", _stdout);
        Assert.Equal($"Database not found: {db}{Environment.NewLine}", _stderr);
    }

    private static string[] Split(string commandLine) => commandLine.Split(' ');
}

/// <summary><see cref="AdminCli.ResetAccount"/> against a temporary SQLite file.</summary>
public sealed class AdminCliResetAccountTests : IDisposable
{
    private const string Key = "ABCD1234";
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "windup-relay-reset-" + Guid.NewGuid().ToString("N"));
    private readonly string _dbPath;
    private readonly StringWriter _out = new();
    private readonly StringWriter _err = new();

    public AdminCliResetAccountTests()
    {
        Directory.CreateDirectory(_dir);
        _dbPath = Path.Combine(_dir, "relay.db");
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        try
        {
            Directory.Delete(_dir, recursive: true);
        }
        catch (IOException)
        {
            // Best effort temp cleanup.
        }
    }

    private static string NewHash() => RelaySecret.HashHex(RelaySecret.Generate())!;

    private int Reset(string rawKey) => AdminCli.ResetAccount(_dbPath, rawKey, _out, _err);

    private long ResetEvents(string key)
    {
        using var conn = new SqliteConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM relay_events WHERE kind = 'account_reset' AND pairing_key = $k;";
        cmd.Parameters.AddWithValue("$k", key);
        return (long)cmd.ExecuteScalar()!;
    }

    [Fact]
    public void Delete_Exits0_RemovesOnlyThatAccount_AndRecordsTheEvent()
    {
        using (var store = new RelayStore(_dbPath))
        {
            store.TryCreateAccount(Key, NewHash());
            store.TryCreateAccount("WXYZ9876", NewHash());
        }

        Assert.Equal(0, Reset(Key));

        using (var store = new RelayStore(_dbPath))
        {
            Assert.Null(store.GetAccountSecretHash(Key));
            Assert.NotNull(store.GetAccountSecretHash("WXYZ9876"));
        }

        Assert.Equal(1, ResetEvents(Key));
        Assert.StartsWith($"Database: {_dbPath}{Environment.NewLine}", _out.ToString());
        Assert.Contains($"Deleted relay account for {Key}", _out.ToString());
    }

    [Theory]
    [InlineData("abcd1234")]
    [InlineData(" ABCD-1234 ")]
    [InlineData("abcd 1234")]
    public void Key_IsNormalized(string rawKey)
    {
        using (var store = new RelayStore(_dbPath))
            store.TryCreateAccount(Key, NewHash());

        Assert.Equal(0, Reset(rawKey));
        using (var store = new RelayStore(_dbPath))
            Assert.Null(store.GetAccountSecretHash(Key));
        Assert.Equal(1, ResetEvents(Key));
    }

    [Fact]
    public void NoAccount_Exits1_AndRecordsNothing()
    {
        using (var store = new RelayStore(_dbPath))
            store.TryCreateAccount("WXYZ9876", NewHash());

        Assert.Equal(1, Reset(Key));
        Assert.Equal(0, ResetEvents(Key));
        Assert.Contains("nothing changed", _out.ToString());
        using var after = new RelayStore(_dbPath);
        Assert.NotNull(after.GetAccountSecretHash("WXYZ9876"));
    }

    [Fact]
    public void MissingDatabase_Exits1_WithoutCreatingIt()
    {
        Assert.Equal(1, Reset(Key));
        Assert.False(File.Exists(_dbPath));
        Assert.Contains("Database not found", _err.ToString());
    }

    [Fact]
    public void RelativePath_IsPrintedInFull_AgainstTheCurrentDirectory()
    {
        var name = "windup-relay-missing-" + Guid.NewGuid().ToString("N") + ".db";
        Assert.Equal(1, AdminCli.ResetAccount(name, Key, _out, _err));
        var full = Path.Combine(Environment.CurrentDirectory, name);
        Assert.StartsWith($"Database: {full}{Environment.NewLine}", _out.ToString());
        Assert.Contains($"Database not found: {full}", _err.ToString());
        Assert.False(File.Exists(full));
    }

    [Theory]
    [InlineData("bad")]
    [InlineData("ABCD12345")]
    [InlineData("")]
    [InlineData("ABCD=1234")]
    [InlineData("ABCD:1234")]
    [InlineData("ABCD/1234")]
    public void InvalidKey_Exits2_WithoutOpeningTheDatabase(string rawKey)
    {
        Assert.Equal(2, Reset(rawKey));
        Assert.False(File.Exists(_dbPath));
        Assert.Contains("Invalid pairing key", _err.ToString());
    }
}
