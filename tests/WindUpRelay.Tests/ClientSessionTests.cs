using System.Net.WebSockets;
using WindUpKey.Protocol;

namespace WindUpRelay.Tests;

/// <summary>
/// <see cref="ClientSession"/>'s send bounds (02-D25), against a socket whose sends never complete on their own:
/// a session that doesn't take a frame in time is aborted, whether its send or the wait for its send lock timed out.
/// </summary>
public sealed class ClientSessionTests
{
    private static readonly TimeSpan Short = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan Long = TimeSpan.FromSeconds(30);

    private static Envelope Frame() => Envelope.Create(MessageTypes.Pong, new { });

    [Fact]
    public async Task AnOwnLoopSendNotTakenInTime_AbortsTheSession()
    {
        var socket = new StuckSocket(honorsCancellation: true);
        var session = new ClientSession(socket, Short);

        var sent = await session.SendAsync(Frame(), CancellationToken.None).WaitAsync(Long);

        Assert.False(sent);
        Assert.True(socket.Aborted);
        Assert.False(session.IsOpen);
    }

    [Fact]
    public async Task AnOwnLoopSendWaitingForTheLockHeldByAStuckSend_AbortsTheSession()
    {
        var socket = new StuckSocket(honorsCancellation: false);
        var session = new ClientSession(socket, Short);
        var holder = session.TrySendAsync(Frame(), Long);
        await socket.SendStarted.WaitAsync(Long);

        var sent = await session.SendAsync(Frame(), CancellationToken.None).WaitAsync(Long);

        Assert.False(sent);
        Assert.True(socket.Aborted);
        Assert.False(await holder.WaitAsync(Long));
    }

    [Fact]
    public async Task ACrossSessionSendTimingOutOnTheSendLock_AbortsTheSession()
    {
        var socket = new StuckSocket(honorsCancellation: false);
        var session = new ClientSession(socket, Long);
        var holder = session.TrySendAsync(Frame(), Long);
        await socket.SendStarted.WaitAsync(Long);
        Assert.False(holder.IsCompleted);

        var started = DateTime.UtcNow;
        var sent = await session.TrySendAsync(Frame(), Short).WaitAsync(Long);

        Assert.False(sent);
        Assert.True(socket.Aborted, "a send that timed out waiting for the lock didn't abort the stuck session");
        Assert.True(DateTime.UtcNow - started < TimeSpan.FromSeconds(5));

        // The stuck send ends with the abort, and later sends are refused at once.
        Assert.False(await holder.WaitAsync(Long));
        Assert.False(await session.TrySendAsync(Frame(), Long).WaitAsync(Short * 5));
    }

    [Fact]
    public async Task ACloseWaitingForTheLockHeldByAStuckSend_AbortsTheSession()
    {
        var socket = new StuckSocket(honorsCancellation: false);
        var session = new ClientSession(socket, Short);
        var holder = session.TrySendAsync(Frame(), Long);
        await socket.SendStarted.WaitAsync(Long);

        await session.CloseAsync(WebSocketCloseStatus.PolicyViolation, "x", CancellationToken.None).WaitAsync(Long);

        Assert.True(socket.Aborted);
        Assert.False(await holder.WaitAsync(Long));
    }

    [Fact]
    public async Task AnOwnLoopSendCancelledByItsOwnToken_Throws_WithoutCountingAsATimeout()
    {
        // Only a real shutdown (the session's or the hub's token) ends a session's loop.
        var socket = new StuckSocket(honorsCancellation: true);
        var session = new ClientSession(socket, Long);
        using var shutdown = new CancellationTokenSource(Short);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => session.SendAsync(Frame(), shutdown.Token).WaitAsync(Long));
        Assert.False(socket.Aborted);
    }

    /// <summary>
    /// A socket whose sends never complete until it's aborted (or, when <paramref name="honorsCancellation"/>, until
    /// the send's token is cancelled, like a real socket whose peer stopped reading).
    /// </summary>
    private sealed class StuckSocket(bool honorsCancellation) : WebSocket
    {
        private readonly TaskCompletionSource _aborted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _sendStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private volatile WebSocketState _state = WebSocketState.Open;

        public bool Aborted => _aborted.Task.IsCompleted;

        public Task SendStarted => _sendStarted.Task;

        public override WebSocketCloseStatus? CloseStatus => null;

        public override string? CloseStatusDescription => null;

        public override WebSocketState State => _state;

        public override string? SubProtocol => null;

        public override void Abort()
        {
            _state = WebSocketState.Aborted;
            _aborted.TrySetResult();
        }

        public override async Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken)
        {
            _sendStarted.TrySetResult();
            var stuck = honorsCancellation
                ? _aborted.Task.WaitAsync(cancellationToken)
                : _aborted.Task;
            await stuck;
            throw new WebSocketException("aborted");
        }

        public override Task CloseAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public override Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public override void Dispose()
        {
        }
    }
}
