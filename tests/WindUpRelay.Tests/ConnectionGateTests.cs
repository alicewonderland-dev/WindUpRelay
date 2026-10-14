using System.Net;

namespace WindUpRelay.Tests;

/// <summary>Connection admission (03-D9): which address a connection counts for, and the caps.</summary>
public sealed class ConnectionGateTests
{
    private static readonly IPAddress Loopback = IPAddress.Loopback;

    [Fact]
    public void ALoopbackPeer_CountsForTheForwardedAddress()
    {
        Assert.Equal("203.0.113.7", ConnectionGate.ClientKey(Loopback, "203.0.113.7"));
        Assert.Equal("203.0.113.7", ConnectionGate.ClientKey(IPAddress.IPv6Loopback, " 203.0.113.7 "));
        Assert.Equal("203.0.113.7", ConnectionGate.ClientKey(IPAddress.Parse("::ffff:127.0.0.1"), "203.0.113.7"));
    }

    [Fact]
    public void OnlyTheLastForwardedEntry_IsTrusted()
    {
        // Entries before the last one were written by whoever connected to the proxy, so a client could pick them.
        Assert.Equal("198.51.100.2", ConnectionGate.ClientKey(Loopback, "203.0.113.7, 198.51.100.2"));
        Assert.Equal("198.51.100.2", ConnectionGate.ClientKey(Loopback, "garbage,198.51.100.2"));
    }

    [Fact]
    public void ANonLoopbackPeer_IsTheClient_AndHeadersAreIgnored()
    {
        var peer = IPAddress.Parse("192.0.2.10");
        Assert.Equal("192.0.2.10", ConnectionGate.ClientKey(peer, "203.0.113.7"));
        Assert.Equal("192.0.2.10", ConnectionGate.ClientKey(IPAddress.Parse("::ffff:192.0.2.10"), "203.0.113.7"));
    }

    [Fact]
    public void ALoopbackPeer_WithoutAUsableHeader_GetsNoPerClientCap()
    {
        Assert.Null(ConnectionGate.ClientKey(Loopback, null));
        Assert.Null(ConnectionGate.ClientKey(Loopback, "  "));
        Assert.Null(ConnectionGate.ClientKey(Loopback, "not-an-address"));
        Assert.Null(ConnectionGate.ClientKey(null, "203.0.113.7"));
    }

    [Fact]
    public void IPv6Clients_AreCountedPerSlash64()
    {
        var a = ConnectionGate.ClientKey(Loopback, "2001:db8:1:2:aaaa::1");
        var b = ConnectionGate.ClientKey(Loopback, "[2001:db8:1:2:bbbb::2]:443");
        var other = ConnectionGate.ClientKey(Loopback, "2001:db8:1:3::1");
        Assert.Equal("2001:db8:1:2::/64", a);
        Assert.Equal(a, b);
        Assert.NotEqual(a, other);
    }

    [Fact]
    public void TheFirstBindKey_GroupsIPv6PerSlash48_AndKeepsIPv4AndTheTrustRules()
    {
        // 05-D7: the first-bind limit's key is found the same way, but a whole site (/48) shares it.
        const int bits = FirstBindLimiter.Ipv6PrefixBits;
        var a = ConnectionGate.ClientKey(Loopback, "2001:db8:1:2:aaaa::1", bits);
        var sameSite = ConnectionGate.ClientKey(Loopback, "[2001:db8:1:ffff::2]:443", bits);
        var otherSite = ConnectionGate.ClientKey(Loopback, "2001:db8:2:2::1", bits);
        Assert.Equal("2001:db8:1::/48", a);
        Assert.Equal(a, sameSite);
        Assert.NotEqual(a, otherSite);

        Assert.Equal("203.0.113.7", ConnectionGate.ClientKey(Loopback, "203.0.113.7", bits));
        Assert.Equal("203.0.113.7", ConnectionGate.ClientKey(Loopback, "::ffff:203.0.113.7", bits));
        Assert.Null(ConnectionGate.ClientKey(Loopback, null, bits));
        Assert.Equal("2001:db8:5::/48", ConnectionGate.ClientKey(IPAddress.Parse("2001:db8:5:6::7"), "203.0.113.7", bits));
    }

    [Fact]
    public void TheCapPerClient_RefusesOnlyThatClient()
    {
        var gate = new ConnectionGate(maxConnections: 100, maxPerClient: 2);
        Assert.Equal(ConnectionAdmission.Admitted, gate.TryEnter("203.0.113.7", out _));
        Assert.Equal(ConnectionAdmission.Admitted, gate.TryEnter("203.0.113.7", out _));

        Assert.Equal(ConnectionAdmission.ClientFull, gate.TryEnter("203.0.113.7", out var refused));
        Assert.Null(refused);
        Assert.Equal(ConnectionAdmission.Admitted, gate.TryEnter("203.0.113.8", out _));
        Assert.Equal(3, gate.OpenConnections);
    }

    [Fact]
    public void ConnectionsWithoutAClientKey_HaveOnlyTheGlobalCap()
    {
        var gate = new ConnectionGate(maxConnections: 5, maxPerClient: 1);
        for (var i = 0; i < 5; i++)
            Assert.Equal(ConnectionAdmission.Admitted, gate.TryEnter(null, out _));

        Assert.Equal(ConnectionAdmission.GlobalFull, gate.TryEnter(null, out _));
        Assert.Equal(ConnectionAdmission.GlobalFull, gate.TryEnter("203.0.113.7", out _));
    }

    [Fact]
    public void ReleasingASlot_FreesItOnce()
    {
        var gate = new ConnectionGate(maxConnections: 2, maxPerClient: 1);
        Assert.Equal(ConnectionAdmission.Admitted, gate.TryEnter("203.0.113.7", out var first));
        Assert.Equal(ConnectionAdmission.Admitted, gate.TryEnter(null, out var second));
        Assert.Equal(ConnectionAdmission.GlobalFull, gate.TryEnter("203.0.113.8", out _));

        first!.Dispose();
        first.Dispose();
        Assert.Equal(1, gate.OpenConnections);
        Assert.Equal(0, gate.OpenFor("203.0.113.7"));
        Assert.Equal(ConnectionAdmission.Admitted, gate.TryEnter("203.0.113.7", out var again));
        Assert.Equal(ConnectionAdmission.GlobalFull, gate.TryEnter(null, out _));

        second!.Dispose();
        again!.Dispose();
        Assert.Equal(0, gate.OpenConnections);
    }
}
