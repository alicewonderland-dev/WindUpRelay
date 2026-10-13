using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using WindUpKey.Protocol;

namespace WindUpRelay.Tests;

/// <summary>The 03-D4 field limits for every payload type the relay forwards, routes, queues or stores.</summary>
public sealed class WireLimitsTests
{
    private const string K = "KEYAAAA1";
    private static readonly string Short = new('r', WireLimits.ShortField);
    private static readonly string ShortPlus = new('r', WireLimits.ShortField + 1);
    private static readonly string Msg = new('m', WireLimits.MessageField);
    private static readonly string MsgPlus = new('m', WireLimits.MessageField + 1);

    private static bool Valid(string type, object payload) =>
        WireLimits.IsValidPayload(type, Envelope.Create(type, payload).Payload);

    /// <summary>Each payload type with a requestId, at the limit and one over.</summary>
    public static TheoryData<string, Func<string, object>> WithRequestId => new()
    {
        { MessageTypes.Wind, r => new WindPayload { RequestId = r, To = K, Hours = 1 } },
        { MessageTypes.WindRequest, r => new WindRequestPayload { RequestId = r, To = K } },
        { MessageTypes.Unwind, r => new UnwindPayload { RequestId = r, To = K } },
        { MessageTypes.WindResult, r => new WindResultPayload { RequestId = r, To = K } },
        { MessageTypes.PairSubmit, r => new PairSubmitPayload { RequestId = r, MyKey = K, TheirKey = "PEERBBB2" } },
        { MessageTypes.PresenceQuery, r => new PresenceQueryPayload { RequestId = r, To = K } },
        { MessageTypes.PresenceResult, r => new PresenceResultPayload { RequestId = r, To = K } },
        { MessageTypes.OwnerSettingsQuery, r => new OwnerSettingsQueryPayload { RequestId = r, To = K } },
        { MessageTypes.OwnerSettingsResult, r => new OwnerSettingsResultPayload { RequestId = r, To = K } },
        { MessageTypes.OwnerSettingsUpdate, r => new OwnerSettingsUpdatePayload { RequestId = r, To = K } },
        { MessageTypes.OwnerSettingsAck, r => new OwnerSettingsAckPayload { RequestId = r, To = K } },
        { MessageTypes.Call, r => new CallPayload { RequestId = r, To = K, WorldId = 1, TerritoryId = 1 } },
        { MessageTypes.CallAck, r => new CallAckPayload { RequestId = r, To = K } },
        { MessageTypes.CallResult, r => new CallResultPayload { RequestId = r, To = K } },
        { MessageTypes.Error, r => new ErrorPayload { RequestId = r, To = K, Code = ErrorCodes.NotAllowed } },
    };

    [Theory]
    [MemberData(nameof(WithRequestId))]
    public void RequestId_UpToTheShortLimit_IsValid_OneMoreIsNot(string type, Func<string, object> payload)
    {
        Assert.True(Valid(type, payload(Short)));
        Assert.False(Valid(type, payload(ShortPlus)));
    }

    /// <summary>Each payload type with a <c>to</c> (and an optional <c>from</c>), given a <c>to</c> and a <c>from</c>.</summary>
    public static TheoryData<string, Func<string, string, object>> WithKeys => new()
    {
        { MessageTypes.Wind, (to, from) => new WindPayload { To = to, From = from, Hours = 1 } },
        { MessageTypes.WindRequest, (to, from) => new WindRequestPayload { To = to, From = from } },
        { MessageTypes.Unwind, (to, from) => new UnwindPayload { To = to, From = from } },
        { MessageTypes.WindResult, (to, from) => new WindResultPayload { To = to, From = from } },
        { MessageTypes.PresenceQuery, (to, from) => new PresenceQueryPayload { To = to, From = from } },
        { MessageTypes.PresenceResult, (to, from) => new PresenceResultPayload { To = to, From = from } },
        { MessageTypes.OwnerGrant, (to, from) => new OwnerGrantPayload { To = to, From = from } },
        { MessageTypes.OwnerRevoked, (to, from) => new OwnerRevokedPayload { To = to, From = from } },
        { MessageTypes.OwnerSettingsQuery, (to, from) => new OwnerSettingsQueryPayload { To = to, From = from } },
        { MessageTypes.OwnerSettingsResult, (to, from) => new OwnerSettingsResultPayload { To = to, From = from } },
        { MessageTypes.OwnerSettingsUpdate, (to, from) => new OwnerSettingsUpdatePayload { To = to, From = from } },
        { MessageTypes.OwnerSettingsAck, (to, from) => new OwnerSettingsAckPayload { To = to, From = from } },
        { MessageTypes.Call, (to, from) => new CallPayload { To = to, From = from, WorldId = 1, TerritoryId = 1 } },
        { MessageTypes.CallAck, (to, from) => new CallAckPayload { To = to, From = from } },
        { MessageTypes.CallResult, (to, from) => new CallResultPayload { To = to, From = from } },
        { MessageTypes.Error, (to, from) => new ErrorPayload { To = to, From = from, Code = ErrorCodes.NotAllowed } },
    };

    [Theory]
    [MemberData(nameof(WithKeys))]
    public void KeyFields_MustNormaliseToEightCharacters(string type, Func<string, string, object> payload)
    {
        Assert.True(Valid(type, payload(K, K)));
        Assert.True(Valid(type, payload(" keya-aaa1 ", "")));
        Assert.False(Valid(type, payload("KEYAAAA", K)));
        Assert.False(Valid(type, payload("KEYAAAA12", K)));
        Assert.False(Valid(type, payload("", K)));
        Assert.False(Valid(type, payload(K + new string('-', 100_000) + "X", K)));

        // from is the relay's to overwrite, but never an oversized or broken one.
        Assert.False(Valid(type, payload(K, "KEY")));
        Assert.False(Valid(type, payload(K, new string('A', 1000))));
    }

    [Fact]
    public void PairKeys_MustNormaliseToEightCharacters()
    {
        Assert.True(Valid(MessageTypes.PairSubmit, new PairSubmitPayload { MyKey = K, TheirKey = "PEERBBB2" }));
        Assert.False(Valid(MessageTypes.PairSubmit, new PairSubmitPayload { MyKey = "KEY", TheirKey = "PEERBBB2" }));
        Assert.False(Valid(MessageTypes.PairSubmit, new PairSubmitPayload { MyKey = K, TheirKey = "PEERBBB22" }));
        Assert.True(Valid(MessageTypes.PairCancel, new PairCancelPayload { PeerKey = K }));
        Assert.False(Valid(MessageTypes.PairCancel, new PairCancelPayload { PeerKey = "nope" }));
        Assert.True(Valid(MessageTypes.PairRemove, new PairRemovePayload { PeerKey = K }));
        Assert.False(Valid(MessageTypes.PairRemove, new PairRemovePayload { PeerKey = K + "9" }));
    }

    [Fact]
    public void ShortTextFields_UpToTheShortLimit_AreValid_OneMoreIsNot()
    {
        Assert.True(Valid(MessageTypes.OwnerGrant, new OwnerGrantPayload { To = K, Identity = Short }));
        Assert.False(Valid(MessageTypes.OwnerGrant, new OwnerGrantPayload { To = K, Identity = ShortPlus }));
        Assert.True(Valid(MessageTypes.OwnerGrant, new OwnerGrantPayload { To = K, Identity = null }));
        Assert.True(Valid(MessageTypes.Call, new CallPayload { To = K, WorldName = Short }));
        Assert.False(Valid(MessageTypes.Call, new CallPayload { To = K, WorldName = ShortPlus }));
        Assert.True(Valid(MessageTypes.WindResult, new WindResultPayload { To = K, RemainingDisplay = Short }));
        Assert.False(Valid(MessageTypes.WindResult, new WindResultPayload { To = K, RemainingDisplay = ShortPlus }));
        Assert.True(Valid(MessageTypes.Error, new ErrorPayload { To = K, Code = Short }));
        Assert.False(Valid(MessageTypes.Error, new ErrorPayload { To = K, Code = ShortPlus }));
        Assert.True(Valid(MessageTypes.CallAck, new CallAckPayload { To = K, Status = Short }));
        Assert.False(Valid(MessageTypes.CallAck, new CallAckPayload { To = K, Status = ShortPlus }));
        Assert.True(Valid(MessageTypes.CallResult, new CallResultPayload { To = K, Status = Short }));
        Assert.False(Valid(MessageTypes.CallResult, new CallResultPayload { To = K, Status = ShortPlus }));
    }

    [Fact]
    public void Messages_UpToTheMessageLimit_AreValid_OneMoreIsNot()
    {
        Assert.True(Valid(MessageTypes.Error, new ErrorPayload { To = K, Code = "x", Message = Msg }));
        Assert.False(Valid(MessageTypes.Error, new ErrorPayload { To = K, Code = "x", Message = MsgPlus }));
        Assert.True(Valid(MessageTypes.CallAck, new CallAckPayload { To = K, Message = Msg }));
        Assert.False(Valid(MessageTypes.CallAck, new CallAckPayload { To = K, Message = MsgPlus }));
        Assert.True(Valid(MessageTypes.CallResult, new CallResultPayload { To = K, Message = Msg }));
        Assert.False(Valid(MessageTypes.CallResult, new CallResultPayload { To = K, Message = MsgPlus }));
    }

    [Fact]
    public void Emotes_UpToTheListLimit_WithShortNames_AreValid()
    {
        static OwnerSettingsResultPayload Result(int count, string name) => new()
        {
            To = K,
            Emotes = Enumerable.Range(0, count).Select(i => new OwnerEmoteInfo { Id = (ushort)i, Name = name }).ToList(),
        };

        Assert.True(Valid(MessageTypes.OwnerSettingsResult, Result(WireLimits.ListEntries, Short)));
        Assert.False(Valid(MessageTypes.OwnerSettingsResult, Result(WireLimits.ListEntries + 1, "a")));
        Assert.False(Valid(MessageTypes.OwnerSettingsResult, Result(1, ShortPlus)));

        var withNull = new JsonObject { ["to"] = K, ["emotes"] = new JsonArray(null, new JsonObject { ["id"] = 1, ["name"] = "a" }) };
        Assert.False(WireLimits.IsValidPayload(MessageTypes.OwnerSettingsResult, withNull));
    }

    [Fact]
    public void DollStatePush_UpToTheListLimit_WithKeysAndShortStamps_IsValid()
    {
        static DollStatePushPayload Push(int count, string key = K, string stamp = "2026-10-13T00:00:00.0000000+00:00") => new()
        {
            Peers = Enumerable.Range(0, count).Select(_ => new PairConsentSyncEntry { PeerKey = key, UpdatedUtc = stamp }).ToList(),
        };

        Assert.True(Valid(MessageTypes.DollStatePush, Push(WireLimits.ListEntries)));
        Assert.False(Valid(MessageTypes.DollStatePush, Push(WireLimits.ListEntries + 1)));
        Assert.False(Valid(MessageTypes.DollStatePush, Push(1, key: "BAD")));
        Assert.True(Valid(MessageTypes.DollStatePush, Push(1, stamp: Short)));
        Assert.False(Valid(MessageTypes.DollStatePush, Push(1, stamp: ShortPlus)));
        Assert.True(Valid(MessageTypes.DollStatePush, new DollStatePushPayload { ExpiryUtc = Short, WindUpdatedUtc = Short }));
        Assert.False(Valid(MessageTypes.DollStatePush, new DollStatePushPayload { ExpiryUtc = ShortPlus }));
        Assert.False(Valid(MessageTypes.DollStatePush, new DollStatePushPayload { WindUpdatedUtc = ShortPlus }));

        var withNull = new JsonObject { ["peers"] = new JsonArray((JsonNode?)null) };
        Assert.False(WireLimits.IsValidPayload(MessageTypes.DollStatePush, withNull));
    }

    [Fact]
    public void Lengths_AreUtf16CodeUnitsAfterUnescaping()
    {
        // 64 escaped characters are 384 characters of JSON, and still within the limit; one more isn't.
        var escaped = JsonNode.Parse($$"""{"requestId":"{{string.Concat(Enumerable.Repeat("\\u00e9", 64))}}","to":"{{K}}"}""")!.AsObject();
        Assert.True(WireLimits.IsValidPayload(MessageTypes.Wind, escaped));
        var over = JsonNode.Parse($$"""{"requestId":"{{string.Concat(Enumerable.Repeat("\\u00e9", 65))}}","to":"{{K}}"}""")!.AsObject();
        Assert.False(WireLimits.IsValidPayload(MessageTypes.Wind, over));

        // A character outside the BMP is two code units.
        Assert.True(Valid(MessageTypes.OwnerGrant, new OwnerGrantPayload { To = K, Identity = string.Concat(Enumerable.Repeat("\U0001F600", 32)) }));
        Assert.False(Valid(MessageTypes.OwnerGrant, new OwnerGrantPayload { To = K, Identity = string.Concat(Enumerable.Repeat("\U0001F600", 32)) + "a" }));
    }

    [Fact]
    public void TypesTheRelayDoesNotRouteOrStore_AndAbsentPayloads_Pass()
    {
        Assert.True(Valid(MessageTypes.Register, new RegisterPayload { PairingKey = "x", Token = new string('t', 1000) }));
        Assert.True(Valid(MessageTypes.Ping, new { }));
        Assert.True(Valid("bogus", new { requestId = new string('r', 1000) }));
        Assert.True(WireLimits.IsValidPayload(MessageTypes.Wind, null));
    }

    [Fact]
    public void APayloadThatDoesNotDeserialise_Throws()
    {
        var wrong = new JsonObject { ["requestId"] = "r", ["to"] = K, ["hours"] = "lots" };
        Assert.ThrowsAny<JsonException>(() => WireLimits.IsValidPayload(MessageTypes.Wind, wrong));
    }

    [Fact]
    public void IsKnown_CoversEveryMessageTypeConstant_AndNothingElse()
    {
        var constants = typeof(MessageTypes).GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral)
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToList();
        Assert.True(constants.Count >= 27);
        Assert.All(constants, c => Assert.True(MessageTypes.IsKnown(c), c));
        Assert.False(MessageTypes.IsKnown("Wind"));
        Assert.False(MessageTypes.IsKnown("bogus"));
        Assert.False(MessageTypes.IsKnown(""));
        Assert.False(MessageTypes.IsKnown(null));
    }

    [Fact]
    public void NormalizeKey_MatchesTheRelaysKeys()
    {
        Assert.Equal("KEYAAAA1", WireLimits.NormalizeKey(" key-aaaa 1 "));
        Assert.Equal(string.Empty, WireLimits.NormalizeKey("   "));
        Assert.Equal(string.Empty, WireLimits.NormalizeKey(null));
        Assert.Equal(SessionHub.NormalizePairingKey("abécd"), WireLimits.NormalizeKey("abécd"));
    }
}
