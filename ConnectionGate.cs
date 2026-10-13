using System.Net;
using System.Net.Sockets;

/// <summary>Why <see cref="ConnectionGate.TryEnter"/> admitted or refused a connection.</summary>
internal enum ConnectionAdmission
{
    Admitted,

    /// <summary><see cref="ConnectionGate.MaxConnections"/> connections are open.</summary>
    GlobalFull,

    /// <summary><see cref="ConnectionGate.MaxPerClient"/> connections from this client's address are open.</summary>
    ClientFull,
}

/// <summary>
/// Connection admission (03-D9): a global cap, and a cap per client address. No sockets or HTTP types, so it can be
/// tested directly. Thread-safe.
/// <para>
/// Behind Tailscale Funnel the TCP peer is always tailscaled on loopback, so the client's address can only come from
/// the header tailscaled adds. <see cref="ClientKey"/> trusts <see cref="ForwardedForHeader"/> only when the TCP peer is
/// loopback, and a connection without a usable one gets no per-client cap at all: a missing header must never put every user into one
/// shared slot of <see cref="MaxPerClient"/>.
/// </para>
/// </summary>
internal sealed class ConnectionGate(int maxConnections, int maxPerClient)
{
    /// <summary>
    /// The only trusted header (03-D15): tailscaled's serve proxy sets (replaces, not appends) it to the Funnel
    /// connection's source address, so a client can't choose it. Any other proxy in front appends, so only its last
    /// entry is trusted. <c>Tailscale-Ingress-Src</c> is never read: a client can send it itself, and tailscaled
    /// doesn't strip it.
    /// </summary>
    public const string ForwardedForHeader = "X-Forwarded-For";

    private readonly object _gate = new();
    private readonly Dictionary<string, int> _byClient = new(StringComparer.Ordinal);
    private int _total;

    public int MaxConnections { get; } = maxConnections;

    public int MaxPerClient { get; } = maxPerClient;

    /// <summary>Open connections admitted by this gate.</summary>
    public int OpenConnections
    {
        get
        {
            lock (_gate)
                return _total;
        }
    }

    /// <summary>Connections currently counted for <paramref name="clientKey"/>.</summary>
    public int OpenFor(string clientKey)
    {
        lock (_gate)
            return _byClient.GetValueOrDefault(clientKey);
    }

    /// <summary>
    /// The per-client key of a connection, or null when it gets no per-client cap. A non-loopback peer is the client
    /// itself, and any header is ignored. A loopback peer (tailscaled, or another local proxy) is trusted for
    /// <see cref="ForwardedForHeader"/> (its last entry) only; without a usable one, null, and only the global cap
    /// applies. IPv6 clients are counted per /64, since one host usually holds a whole /64; the first-bind limit groups
    /// them more widely (<paramref name="ipv6PrefixBits"/>, 05-D7).
    /// </summary>
    public static string? ClientKey(IPAddress? peer, string? forwardedFor, int ipv6PrefixBits = 64)
    {
        if (peer is null)
            return null;
        if (peer.IsIPv4MappedToIPv6)
            peer = peer.MapToIPv4();
        if (!IPAddress.IsLoopback(peer))
            return Bucket(peer, ipv6PrefixBits);

        var client = LastForwarded(forwardedFor);
        return client is null ? null : Bucket(client, ipv6PrefixBits);
    }

    /// <summary>
    /// Counts a new connection for <paramref name="clientKey"/> (null: global cap only). Admitted connections get a
    /// <paramref name="slot"/>, which must be disposed when the session ends, on every path; disposing it twice releases
    /// once.
    /// </summary>
    public ConnectionAdmission TryEnter(string? clientKey, out IDisposable? slot)
    {
        slot = null;
        lock (_gate)
        {
            if (_total >= MaxConnections)
                return ConnectionAdmission.GlobalFull;
            if (clientKey is not null && _byClient.GetValueOrDefault(clientKey) >= MaxPerClient)
                return ConnectionAdmission.ClientFull;

            _total++;
            if (clientKey is not null)
                _byClient[clientKey] = _byClient.GetValueOrDefault(clientKey) + 1;
        }

        slot = new Slot(this, clientKey);
        return ConnectionAdmission.Admitted;
    }

    private void Release(string? clientKey)
    {
        lock (_gate)
        {
            _total--;
            if (clientKey is null)
                return;
            if (_byClient.TryGetValue(clientKey, out var open) && open > 1)
                _byClient[clientKey] = open - 1;
            else
                _byClient.Remove(clientKey);
        }
    }

    private static IPAddress? LastForwarded(string? header)
    {
        if (string.IsNullOrWhiteSpace(header))
            return null;
        var last = header.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).LastOrDefault();
        return ParseAddress(last);
    }

    /// <summary>An address written plain (<c>1.2.3.4</c>, <c>2001:db8::1</c>) or with a port (<c>1.2.3.4:5</c>, <c>[2001:db8::1]:5</c>).</summary>
    private static IPAddress? ParseAddress(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            return null;
        raw = raw.Trim();
        if (IPAddress.TryParse(raw, out var address) || (IPEndPoint.TryParse(raw, out var endPoint) && (address = endPoint.Address) is not null))
            return address.IsIPv4MappedToIPv6 ? address.MapToIPv4() : address;
        return null;
    }

    /// <summary>An IPv4 address as is; an IPv6 address as its /<paramref name="prefixBits"/> network.</summary>
    private static string Bucket(IPAddress address, int prefixBits)
    {
        if (address.AddressFamily != AddressFamily.InterNetworkV6)
            return address.ToString();

        prefixBits = Math.Clamp(prefixBits, 0, 128);
        var bytes = address.GetAddressBytes();
        for (var i = 0; i < bytes.Length; i++)
        {
            var keep = Math.Clamp(prefixBits - (i * 8), 0, 8);
            bytes[i] &= (byte)(0xFF << (8 - keep));
        }

        return new IPAddress(bytes) + "/" + prefixBits.ToString(System.Globalization.CultureInfo.InvariantCulture);
    }

    private sealed class Slot(ConnectionGate gate, string? clientKey) : IDisposable
    {
        private int _released;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0)
                gate.Release(clientKey);
        }
    }
}
