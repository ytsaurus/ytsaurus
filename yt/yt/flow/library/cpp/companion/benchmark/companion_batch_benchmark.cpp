// Per-epoch cost of packing read-only joined external states into a
// companion request: AddJoinedExternalStates over a batch of messages,
// with and without a key schema override. The override case pays a
// payload-to-key conversion per message inside ExtractKeys — this bounds
// the overhead an ordered source with a joiner pays before the companion
// round trip.
//
// Args: {message count} in {100, 10000}; items/sec = messages.

#include <yt/yt/flow/library/cpp/companion/companion_computation_base.h>
#include <yt/yt/flow/library/cpp/companion/state_codec.h>

#include <yt/yt/flow/library/cpp/companion/proto/companion_service.pb.h>

#include <yt/yt/flow/library/cpp/common/companion_state_adapter.h>
#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/payload_converter.h>
#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/common/state_provider.h>

#include <yt/yt/flow/library/cpp/misc/compact_unversioned_owning_row.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ytree/convert.h>

#include <benchmark/benchmark.h>

namespace NYT::NFlow::NCompanion {
namespace {

using namespace NTableClient;
using namespace NYTree;
using namespace NYson;
using benchmark::DoNotOptimize;

////////////////////////////////////////////////////////////////////////////////

class TBenchJoinedStateKeyProvider
    : public IJoinedStateKeyProvider
{
public:
    THashMap<TKey, IStateHolderPtr> States;

    TBenchJoinedStateKeyProvider(
        TTableSchemaPtr keySchema,
        bool hasKeySchemaOverride)
        : KeySchema_(std::move(keySchema))
        , HasKeySchemaOverride_(hasKeySchemaOverride)
        , ConverterCache_(CreatePayloadConverterCache(
            NQueryClient::CreateColumnEvaluatorCache(New<NQueryClient::TColumnEvaluatorCacheConfig>())))
    { }

    IStateHolderPtr GetState(const TKey& key) override
    {
        auto it = States.find(key);
        return it == States.end() ? nullptr : it->second;
    }

    TFuture<void> PreloadKeyStates(const THashSet<TKey>& /*keys*/) override
    {
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

// Read-only proto-style adapter: extract-derived keys, marker payload for
// keys the provider holds a state for.
class TBenchJoinedStateAdapter
    : public ICompanionStateAdapter
{
public:
    explicit TBenchJoinedStateAdapter(TIntrusivePtr<TBenchJoinedStateKeyProvider> provider)
        : Provider_(std::move(provider))
    { }

    TCompanionStateDescriptor Describe() const final
    {
        return TCompanionStateDescriptor{
            .StateName = "/joined",
        };
    }

    TSharedRef EncodeState(const TKey& key) final
    {
        return Provider_->GetState(key)
            ? TSharedRef::FromString(std::string("joined-state-payload"))
            : TSharedRef();
    }

    void ApplyState(const TKey& /*key*/, TSharedRef /*payload*/) final
    {
        YT_ABORT();
    }

    void ResetState(const TKey& /*key*/) final
    {
        YT_ABORT();
    }

    THashSet<TKey> ExtractKeys(const IInputContextPtr& input) const final
    {
        return ExtractJoinedStateKeys(*Provider_, input);
    }

private:
    const TIntrusivePtr<TBenchJoinedStateKeyProvider> Provider_;
};

////////////////////////////////////////////////////////////////////////////////

const auto PayloadSchema = ConvertTo<TTableSchemaPtr>(TYsonStringBuf(
    R"""([{name="word"; type="string";};])"""));

// The override re-extracts the key from the payload under this schema.
const auto OverrideKeySchema = ConvertTo<TTableSchemaPtr>(TYsonStringBuf(
    R"""([{name="word"; type="string"; sort_order="ascending";};])"""));

constexpr auto ValidTs = TSystemTimestamp(1'500'000'000);

TInputMessageConstPtr MakeMessage(int index)
{
    auto word = "word-" + ToString(index);
    TMessageBuilder builder(TStreamId("in"), PayloadSchema);
    builder.Payload().SetValue(MakeUnversionedStringValue(word, 0));
    builder.SetMessageId(TMessageId(word));
    builder.SetSystemTimestamp(ValidTs);
    builder.SetAlignmentTimestamp(ValidTs);
    builder.SetEventTimestamp(ValidTs);
    return New<TInputMessage>(builder.Finish(), MakeKey(TStringBuf(word)));
}

IInputContextPtr MakeInput(int messageCount)
{
    std::vector<TInputMessageConstPtr> messages;
    messages.reserve(messageCount);
    for (int index = 0; index < messageCount; ++index) {
        messages.push_back(MakeMessage(index));
    }
    return New<TInputContext>(
        std::move(messages),
        std::vector<TInputTimerConstPtr>{},
        std::vector<TInputVisitConstPtr>{});
}

void RunAddJoinedExternalStates(benchmark::State& state, bool withOverride)
{
    auto messageCount = state.range(0);
    auto provider = New<TBenchJoinedStateKeyProvider>(OverrideKeySchema, withOverride);
    auto adapter = New<TBenchJoinedStateAdapter>(provider);
    auto input = MakeInput(messageCount);

    // A state exists for every extract-derived key, so every message
    // contributes one encoded item.
    for (const auto& key : adapter->ExtractKeys(input)) {
        provider->States[key] = New<NFlow::TStateHolder<TSimpleExternalState>>();
    }

    THashMap<std::string, ICompanionStateAdapterPtr> joiners;
    joiners.emplace("/joined", adapter);

    for (auto _ : state) {
        auto request = New<TCompanionProcessRequest>();
        AddJoinedExternalStates(request, joiners, input);
        Y_ABORT_UNLESS(
            std::ssize(GetOrCrash(request->JoinedExternalStates, "/joined").StateItems) ==
            messageCount);
        DoNotOptimize(request.Get());
    }
    state.SetItemsProcessed(state.iterations() * messageCount);
}

void BM_AddJoinedExternalStates(benchmark::State& state)
{
    RunAddJoinedExternalStates(state, /*withOverride*/ false);
}

void BM_AddJoinedExternalStatesOverride(benchmark::State& state)
{
    RunAddJoinedExternalStates(state, /*withOverride*/ true);
}

BENCHMARK(BM_AddJoinedExternalStates)->Arg(100)->Arg(10000);
BENCHMARK(BM_AddJoinedExternalStatesOverride)->Arg(100)->Arg(10000);

////////////////////////////////////////////////////////////////////////////////

// A SimpleRow external-state payload across the type-erased wire boundary,
// at realistic payload sizes: encode the row the way the manager adapter
// does, pack the shared ref into the request wire message, parse a response
// wire message back and land the bytes in a row the way ApplyState does.
// This is the per-item chain that replaced the typed TStateHolder<TPayload>
// path; the erased payload must not add full-payload copies to it.
//
// Args: {payload bytes} in {1 KiB, 100 KiB}; bytes/sec = payload volume.

const auto StateSchema = ConvertTo<TTableSchemaPtr>(TYsonStringBuf(
    R"""([{name="data"; type="string";};])"""));

void BM_ExternalStateWireRoundTrip(benchmark::State& state)
{
    auto payloadBytes = state.range(0);

    TPayloadBuilder builder(StateSchema);
    builder.Set<TStringBuf>(std::string(payloadBytes, 'x'), "data");
    auto payload = builder.Finish();
    auto key = MakeKey(TStringBuf("word-0"));

    for (auto _ : state) {
        // Request: the adapter's encode plus the holder-to-wire packing.
        auto encoded = TSharedRef::FromString(ToProto<TProtobufString>(payload));
        TStateHolder<TSharedRef> holder;
        holder.StateName = "/state";
        holder.StateItems.push_back({.Key = key, .State = std::move(encoded)});
        NProto::NCompanion::TState wire;
        SerializeStateHolder(&wire, holder, EStateDirection::Request);

        // Response: wire-to-holder parsing plus the adapter's apply.
        auto parsed = ParseStateHolder<TSharedRef>(wire, EStateDirection::Response);
        const auto& bytes = parsed.StateItems[0].State;
        TCompactUnversionedOwningRow row;
        DeserializeFromBuffer(bytes.Begin(), bytes.End(), &row);
        DoNotOptimize(row.GetCount());
    }
    state.SetBytesProcessed(state.iterations() * payloadBytes);
}

BENCHMARK(BM_ExternalStateWireRoundTrip)->Arg(1 << 10)->Arg(100 << 10);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
