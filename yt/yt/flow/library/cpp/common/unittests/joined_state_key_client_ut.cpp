#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload_converter.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/state_provider.h>
#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/common/visit.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TFakeJoinedStateKeyProvider
    : public IJoinedStateKeyProvider
{
public:
    THashSet<TKey> PreloadedKeys;

    TFakeJoinedStateKeyProvider(
        TTableSchemaPtr keySchema,
        bool hasKeySchemaOverride,
        std::optional<THashSet<TStreamId>> keyProviderStreams)
        : KeySchema_(std::move(keySchema))
        , HasKeySchemaOverride_(hasKeySchemaOverride)
        , KeyProviderStreams_(std::move(keyProviderStreams))
        , ConverterCache_(CreatePayloadConverterCache(
            NQueryClient::CreateColumnEvaluatorCache(New<NQueryClient::TColumnEvaluatorCacheConfig>())))
    { }

    IStateHolderPtr GetState(const TKey& /*key*/) override
    {
        return nullptr;
    }

    TFuture<void> PreloadKeyStates(const THashSet<TKey>& keys) override
    {
        PreloadedKeys = keys;
        return OKFuture;
    }

    TTableSchemaPtr GetKeySchema() const override
    {
        return KeySchema_;
    }

    const IPayloadConverterCachePtr& GetConverterCache() const override
    {
        return ConverterCache_;
    }

    const std::optional<THashSet<TStreamId>>& GetKeyProviderStreams() const override
    {
        return KeyProviderStreams_;
    }

    bool HasKeySchemaOverride() const override
    {
        return HasKeySchemaOverride_;
    }

private:
    const TTableSchemaPtr KeySchema_;
    const bool HasKeySchemaOverride_;
    const std::optional<THashSet<TStreamId>> KeyProviderStreams_;
    const IPayloadConverterCachePtr ConverterCache_;
};

////////////////////////////////////////////////////////////////////////////////

const auto PayloadSchema = ConvertTo<TTableSchemaPtr>(TYsonStringBuf(
    R"""([{name="word"; type="string";};])"""));

const auto OverrideKeySchema = ConvertTo<TTableSchemaPtr>(TYsonStringBuf(
    R"""([
        {name="hash"; type="uint64"; expression="farm_hash(word)"; required=%true; sort_order="ascending";};
        {name="word"; type="string"; sort_order="ascending";};
    ])"""));

constexpr auto ValidTs = TSystemTimestamp(1'500'000'000);

TInputMessageConstPtr MakeMessage(const std::string& streamId, const std::string& word, const TKey& key)
{
    TMessageBuilder builder(TStreamId(streamId), PayloadSchema);
    builder.Payload().SetValue(MakeUnversionedStringValue(word, 0));
    builder.SetMessageId(TMessageId(word));
    builder.SetSystemTimestamp(ValidTs);
    builder.SetAlignmentTimestamp(ValidTs);
    builder.SetEventTimestamp(ValidTs);
    return New<TInputMessage>(builder.Finish(), key);
}

TInputTimerConstPtr MakeTimer(const std::string& streamId, const std::string& word)
{
    TTimer timer;
    timer.MessageId = TMessageId("timer-" + word);
    timer.StreamId = TStreamId(streamId);
    timer.SystemTimestamp = ValidTs;
    timer.AlignmentTimestamp = ValidTs;
    timer.EventTimestamp = ValidTs;
    timer.TriggerTimestamp = ValidTs;
    timer.Key = MakeKey(word);
    timer.KeySchema = PayloadSchema;
    return New<TInputTimer>(std::move(timer), PayloadSchema);
}

TInputVisitConstPtr MakeVisit(const std::string& streamId, const TKey& key)
{
    TVisit visit;
    visit.MessageId = TMessageId("visit");
    visit.StreamId = TStreamId(streamId);
    visit.SystemTimestamp = ValidTs;
    visit.AlignmentTimestamp = ValidTs;
    visit.EventTimestamp = ValidTs;
    visit.Key = key;
    return New<TInputVisit>(std::move(visit));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TJoinedStateKeyClientTest, ExtractKeysVerbatimWithoutOverride)
{
    auto provider = New<TFakeJoinedStateKeyProvider>(
        PayloadSchema,
        /*hasKeySchemaOverride*/ false,
        /*keyProviderStreams*/ std::nullopt);
    TJoinedStateKeyClient<void> client(provider);

    auto input = New<TInputContext>(
        std::vector<TInputMessageConstPtr>{MakeMessage("in", "abc", MakeKey("abc"))},
        std::vector<TInputTimerConstPtr>{MakeTimer("timers", "def")},
        std::vector<TInputVisitConstPtr>{MakeVisit("visits", MakeKey("ghi"))});

    auto keys = client.ExtractKeys(input);

    EXPECT_EQ(keys, (THashSet<TKey>{MakeKey("abc"), MakeKey("def"), MakeKey("ghi")}));
}

TEST(TJoinedStateKeyClientTest, ExtractKeysSkipsNonProviderStreams)
{
    auto provider = New<TFakeJoinedStateKeyProvider>(
        PayloadSchema,
        /*hasKeySchemaOverride*/ false,
        /*keyProviderStreams*/ THashSet<TStreamId>{TStreamId("in")});
    TJoinedStateKeyClient<void> client(provider);

    auto input = New<TInputContext>(
        std::vector<TInputMessageConstPtr>{
            MakeMessage("in", "abc", MakeKey("abc")),
            MakeMessage("other", "def", MakeKey("def")),
        },
        std::vector<TInputTimerConstPtr>{MakeTimer("timers", "ghi")},
        std::vector<TInputVisitConstPtr>{MakeVisit("visits", MakeKey("jkl"))});

    auto keys = client.ExtractKeys(input);

    EXPECT_EQ(keys, (THashSet<TKey>{MakeKey("abc")}));
}

TEST(TJoinedStateKeyClientTest, ExtractKeysAppliesOverrideLikeResolveKey)
{
    auto provider = New<TFakeJoinedStateKeyProvider>(
        OverrideKeySchema,
        /*hasKeySchemaOverride*/ true,
        /*keyProviderStreams*/ std::nullopt);
    TJoinedStateKeyClient<void> client(provider);

    auto message = MakeMessage("in", "abc", MakeKey("group-by-key"));
    auto timer = MakeTimer("timers", "def");
    auto visitKey = MakeKey(ui64(7), "bucket");
    auto input = New<TInputContext>(
        std::vector<TInputMessageConstPtr>{message},
        std::vector<TInputTimerConstPtr>{timer},
        std::vector<TInputVisitConstPtr>{MakeVisit("visits", visitKey)});

    auto keys = client.ExtractKeys(input);

    // Messages and timers land in the override key space, exactly as the per-item resolution;
    // visits carry their key verbatim.
    EXPECT_EQ(keys, (THashSet<TKey>{client.ResolveKey(message), client.ResolveKey(timer), visitKey}));
    EXPECT_FALSE(keys.contains(MakeKey("group-by-key")));
}

TEST(TJoinedStateKeyClientTest, PreloadLoadsExactlyExtractedKeys)
{
    auto provider = New<TFakeJoinedStateKeyProvider>(
        OverrideKeySchema,
        /*hasKeySchemaOverride*/ true,
        /*keyProviderStreams*/ THashSet<TStreamId>{TStreamId("in")});
    TJoinedStateKeyClient<void> client(provider);

    auto input = New<TInputContext>(
        std::vector<TInputMessageConstPtr>{
            MakeMessage("in", "abc", MakeKey("abc")),
            MakeMessage("other", "def", MakeKey("def")),
        },
        std::vector<TInputTimerConstPtr>{},
        std::vector<TInputVisitConstPtr>{});

    WaitFor(client.PreloadKeyStates(input))
        .ThrowOnError();

    EXPECT_EQ(provider->PreloadedKeys, client.ExtractKeys(input));
    EXPECT_EQ(std::ssize(provider->PreloadedKeys), 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
