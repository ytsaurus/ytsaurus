#include "test_runtime_context.h"

#include "entity_builders.h"

#include <yt/yt/flow/library/cpp/process_function/host/computation_runtime_context.h>

#include <yt/yt/flow/library/cpp/common/column_evaluator_cache.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/payload_converter.h>
#include <yt/yt/flow/library/cpp/common/schema.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/distributed_throttler/factory.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <util/generic/map.h>

namespace NYT::NFlow::NTesting {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

//! Hands out unlimited throttlers so tests never block on quota.
class TUnlimitedThrottlerFactory
    : public NDistributedThrottler::IDistributedThrottlerFactory
{
public:
    IThroughputThrottlerPtr GetClient(std::string_view /*throttlerName*/) override
    {
        return GetUnlimitedThrottler();
    }

    void SetPriority(NDistributedThrottler::TPriority /*priority*/) override
    { }

    void Reconfigure(THashMap<NDistributedThrottler::TThrottlerId, TDynamicThrottlerSpecPtr> /*throttlers*/) override
    { }
};

TWatermarkStatePtr MakeWatermarkState(const THashMap<TStreamId, TSystemTimestamp>& watermarks)
{
    auto watermarkState = New<TWatermarkState>();
    for (const auto& [streamId, eventWatermark] : watermarks) {
        auto streamWatermarks = New<TWatermarks>();
        streamWatermarks->EventWatermark = eventWatermark;
        watermarkState->Streams[streamId] = std::move(streamWatermarks);
    }
    return watermarkState;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TTestRuntimeContextBuilder& TTestRuntimeContextBuilder::SetWatermark(const TStreamId& streamId, TSystemTimestamp value)
{
    Watermarks_[streamId] = value;
    return *this;
}

TTestRuntimeContextBuilder& TTestRuntimeContextBuilder::SetKeySchema(TTableSchemaPtr schema)
{
    KeySchema_ = std::move(schema);
    return *this;
}

TTestRuntimeContextBuilder& TTestRuntimeContextBuilder::SetSpec(TComputationSpecPtr spec)
{
    Spec_ = std::move(spec);
    return *this;
}

TTestRuntimeContextBuilder& TTestRuntimeContextBuilder::SetDynamicParametersNode(IMapNodePtr node)
{
    DynamicParametersNode_ = std::move(node);
    return *this;
}

IRuntimeContextPtr TTestRuntimeContextBuilder::Build() const
{
    auto keySchema = KeySchema_ ? KeySchema_ : DefaultTestKeySchema();

    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> streamSpecsMap;
    i64 nextSpecId = 0;
    for (const auto& [streamId, spec] : Streams_) {
        streamSpecsMap[streamId].emplace(TStreamSpecId(nextSpecId++), spec);
    }

    auto converterCache = CreatePayloadConverterCache(CreateFastColumnEvaluatorCache());
    auto streamSpecs = New<TComputationStreamSpecStorage>(
        New<TStreamSpecs>(streamSpecsMap),
        keySchema,
        converterCache);

    auto spec = Spec_ ? Spec_ : New<TComputationSpec>();
    if (!spec->GroupBySchema) {
        spec->GroupBySchema = keySchema;
    }
    if (spec->OutputStreamIds.empty()) {
        for (const auto& [streamId, streamSpec] : Streams_) {
            spec->OutputStreamIds.insert(streamId);
        }
    }

    // Reuse the production runtime context so tests track production behavior exactly.
    auto context = New<TComputationRuntimeContext>(
        std::move(spec),
        std::move(streamSpecs),
        std::move(keySchema),
        std::move(converterCache),
        New<TUnlimitedThrottlerFactory>());
    context->RefreshEpochState(MakeWatermarkState(Watermarks_), DynamicParametersNode_);
    return context;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting
