using System.Net.WebSockets;
using System.Text;
using WindUpKey.Protocol;

namespace WindUpRelay.Tests;

/// <summary>The shared bounded reader (03-D1) that both receive loops use, over in-memory sockets.</summary>
public sealed class BoundedMessageReaderTests : IDisposable
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);
    private readonly WebSocket _server;
    private readonly WebSocket _client;
    private readonly BoundedMessageReader _reader = new();

    public BoundedMessageReaderTests() => (_server, _client) = InMemorySockets.CreatePair();

    public void Dispose()
    {
        _server.Dispose();
        _client.Dispose();
    }

    private Task SendFramesAsync(WebSocketMessageType type, params byte[][] frames) => Task.Run(async () =>
    {
        for (var i = 0; i < frames.Length; i++)
            await _client.SendAsync(frames[i], type, i == frames.Length - 1, CancellationToken.None);
    });

    private Task SendTextAsync(string text) => SendFramesAsync(WebSocketMessageType.Text, Encoding.UTF8.GetBytes(text));

    private async Task<WireMessage> ReadAsync(int limit) => await _reader.ReadAsync(_server, limit, CancellationToken.None).WaitAsync(Wait);

    [Theory]
    [InlineData(4 * 1024)]
    [InlineData(64 * 1024)]
    public async Task AMessageOfExactlyTheLimit_IsAccepted(int limit)
    {
        var text = new string('a', limit);
        var send = SendTextAsync(text);

        var message = await ReadAsync(limit);

        Assert.Equal(WireMessageKind.Text, message.Kind);
        Assert.Equal(text, message.Text);
        await send;
    }

    [Theory]
    [InlineData(4 * 1024)]
    [InlineData(64 * 1024)]
    public async Task OneByteOverTheLimit_IsOversized(int limit)
    {
        var send = SendTextAsync(new string('a', limit + 1));

        var message = await ReadAsync(limit);

        Assert.Equal(WireMessageKind.Oversized, message.Kind);
        Assert.Null(message.Text);
        await send;
    }

    [Fact]
    public async Task ExactlyTheLimit_FollowedByAnEmptyFinalFrame_IsAccepted()
    {
        var send = SendFramesAsync(WebSocketMessageType.Text, Encoding.UTF8.GetBytes(new string('a', 1024)), []);

        var message = await ReadAsync(1024);

        Assert.Equal(WireMessageKind.Text, message.Kind);
        Assert.Equal(1024, message.Text!.Length);
        await send;
    }

    [Fact]
    public async Task AnOversizedMessage_IsDiscardedToItsEnd_SoTheNextMessageReadsWhole()
    {
        var frames = Enumerable.Range(0, 40).Select(_ => Encoding.UTF8.GetBytes(new string('x', 8 * 1024))).ToArray();
        var send = Task.Run(async () =>
        {
            await SendFramesAsync(WebSocketMessageType.Text, frames);
            await SendTextAsync("next");
        });

        Assert.Equal(WireMessageKind.Oversized, (await ReadAsync(64 * 1024)).Kind);
        var next = await ReadAsync(64 * 1024);

        Assert.Equal(WireMessageKind.Text, next.Kind);
        Assert.Equal("next", next.Text);
        await send;
    }

    [Fact]
    public async Task ACharacterSplitAcrossFrames_Survives()
    {
        // Two-, three- and four-byte characters, sent one byte per frame, so every character is split.
        const string text = "aé€\U0001F600z";
        var frames = Encoding.UTF8.GetBytes(text).Select(b => new[] { b }).ToArray();
        var send = SendFramesAsync(WebSocketMessageType.Text, frames);

        var message = await ReadAsync(1024);

        Assert.Equal(WireMessageKind.Text, message.Kind);
        Assert.Equal(text, message.Text);
        await send;
    }

    [Fact]
    public async Task AMessageBiggerThanTheFirstBuffer_GrowsItUpToTheLimit()
    {
        var text = string.Concat(Enumerable.Range(0, 5000).Select(i => i.ToString("D8")));
        var send = SendFramesAsync(WebSocketMessageType.Text, Encoding.UTF8.GetBytes(text).Chunk(3000).ToArray());

        var message = await ReadAsync(64 * 1024);

        Assert.Equal(text, message.Text);
        await send;
    }

    [Fact]
    public async Task ABinaryMessage_IsReportedAsSuch_AndTheNextReadsWhole()
    {
        var send = Task.Run(async () =>
        {
            await SendFramesAsync(WebSocketMessageType.Binary, new byte[100], new byte[100 * 1024]);
            await SendTextAsync("after");
        });

        Assert.Equal(WireMessageKind.Binary, (await ReadAsync(4 * 1024)).Kind);
        Assert.Equal("after", (await ReadAsync(4 * 1024)).Text);
        await send;
    }

    [Fact]
    public async Task ACloseFrame_IsReportedWithItsStatusAndDescription()
    {
        var send = _client.CloseOutputAsync(WebSocketCloseStatus.PolicyViolation, "because", CancellationToken.None);

        var message = await ReadAsync(1024);

        Assert.Equal(WireMessageKind.Close, message.Kind);
        Assert.Equal(WebSocketCloseStatus.PolicyViolation, message.CloseStatus);
        Assert.Equal("because", message.CloseDescription);
        await send;
    }

    // ---- 03-D16: stopAtLimit, for the relay, which closes as soon as a message crosses its limit ----

    /// <summary>Sends text frames of <paramref name="frameBytes"/> without ever ending the message, until the socket fails.</summary>
    private Task SendEndlessAsync(WebSocketMessageType type, int frameBytes) => Task.Run(async () =>
    {
        var frame = new byte[frameBytes];
        Array.Fill(frame, (byte)'x');
        try
        {
            while (true)
                await _client.SendAsync(frame, type, false, CancellationToken.None);
        }
        catch (Exception ex) when (ex is WebSocketException or IOException or ObjectDisposedException or OperationCanceledException)
        {
            // The test disposed the sockets.
        }
    });

    [Theory]
    [InlineData(4 * 1024)]
    [InlineData(64 * 1024)]
    public async Task StopAtLimit_ReportsAMessageThatNeverEnds_AsOversizedOnceItCrossesTheLimit(int limit)
    {
        var reader = new BoundedMessageReader(stopAtLimit: true);
        _ = SendEndlessAsync(WebSocketMessageType.Text, 1024);

        var message = await reader.ReadAsync(_server, limit, CancellationToken.None).WaitAsync(Wait);

        Assert.Equal(WireMessageKind.Oversized, message.Kind);
        Assert.Null(message.Text);
    }

    [Fact]
    public async Task StopAtLimit_StillAcceptsExactlyTheLimit_AndOneByteMoreIsOversized()
    {
        var reader = new BoundedMessageReader(stopAtLimit: true);
        var text = new string('a', 1024);
        var send = Task.Run(async () =>
        {
            await SendFramesAsync(WebSocketMessageType.Text, Encoding.UTF8.GetBytes(text), []);
            await SendTextAsync(text + "b");
        });

        var exact = await reader.ReadAsync(_server, 1024, CancellationToken.None).WaitAsync(Wait);
        var over = await reader.ReadAsync(_server, 1024, CancellationToken.None).WaitAsync(Wait);

        Assert.Equal(text, exact.Text);
        Assert.Equal(WireMessageKind.Oversized, over.Kind);
        await send;
    }

    [Fact]
    public async Task StopAtLimit_ReportsABinaryMessage_AtItsFirstFrame()
    {
        var reader = new BoundedMessageReader(stopAtLimit: true);
        _ = SendEndlessAsync(WebSocketMessageType.Binary, 100);

        var message = await reader.ReadAsync(_server, 4 * 1024, CancellationToken.None).WaitAsync(Wait);

        Assert.Equal(WireMessageKind.Binary, message.Kind);
    }
}
