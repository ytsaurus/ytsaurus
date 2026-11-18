#include "stream_buffer_duration.h"

#include "registry.h"

#include <yt/yt/core/misc/collection_helpers.h>

#include <algorithm>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TStreamBufferDurationSnapshotPtr BuildStreamBufferDurationSnapshot(
    const TPipelineSpecPtr& spec,
    const TDynamicPipelineSpecPtr& dynamicSpec,
    TInputAcknowledgementPolicyLookup lookup)
{
    struct TPredecessor
    {
        TStreamId StreamId;
        double BatchSeconds = 0;
    };

    struct TNode
    {
        double ProducerBatchSeconds = 0;
        double DownstreamBatchSeconds = 0;
        const TComputationSpec* DependentOutputProducer = nullptr;
        i64 Pending = 0;
        std::vector<TPredecessor> Predecessors;
    };

    if (!lookup) {
        lookup = [] (const std::string& name) {
            return TRegistry::Get()->GetInputAcknowledgementPolicy(name);
        };
    }

    auto result = std::make_shared<TStreamBufferDurationSnapshot>();
    THashMap<TStreamId, TNode> nodes;
    const auto defaultDynamicSpec = New<TDynamicComputationSpec>();
    for (const auto& [computationId, computation] : spec->Computations) {
        auto it = dynamicSpec->Computations.find(computationId);
        const double batchSeconds = std::max(0.0, (it == dynamicSpec->Computations.end() ? defaultDynamicSpec : it->second)->BatchDuration.SecondsFloat());
        result->InputDurations[computationId].ChainSeconds = batchSeconds;
        const auto policy = computation->InputStreamIds.empty()
            ? EInputAcknowledgementPolicy::Unknown
            : lookup(computation->ComputationClassName);

        YT_VERIFY(computation->InputStreamIds.empty() || policy != EInputAcknowledgementPolicy::Unknown);

        for (const auto& streamId : computation->OutputStreamIds) {
            auto& node = nodes[streamId];
            node.ProducerBatchSeconds = batchSeconds;
            if (policy == EInputAcknowledgementPolicy::AfterDependentOutputs) {
                node.DependentOutputProducer = computation.Get();
            }
        }

        for (const auto& streamId : computation->InputStreamIds) {
            auto& node = nodes[streamId];
            node.DownstreamBatchSeconds = std::max(node.DownstreamBatchSeconds, batchSeconds);
        }
    }

    // Only dependent outputs carry input acknowledgements through the shared stream graph.
    for (const auto& [parent, outputs] : BuildStreamGraph(spec)) {
        for (const auto& output : outputs) {
            auto it = nodes.find(output);
            if (it == nodes.end() || !it->second.DependentOutputProducer ||
                !it->second.DependentOutputProducer->InputStreamIds.contains(parent))
            {
                continue;
            }

            ++GetOrCrash(nodes, parent).Pending;
            it->second.Predecessors.push_back({parent, it->second.ProducerBatchSeconds});
        }
    }

    std::vector<TStreamId> ready;
    for (const auto& [streamId, node] : nodes) {
        if (node.Pending == 0) {
            ready.push_back(streamId);
        }
    }

    const double maxSeconds = TDuration::Max().SecondsFloat();
    while (!ready.empty()) {
        auto streamId = std::move(ready.back());
        ready.pop_back();
        const auto& node = GetOrCrash(nodes, streamId);
        const TStreamBufferDurationEstimate estimate{
            .ChainSeconds = std::min(maxSeconds, node.ProducerBatchSeconds + node.DownstreamBatchSeconds),
        };
        result->Streams.emplace(streamId, estimate);

        for (const auto& predecessor : node.Predecessors) {
            auto& parent = GetOrCrash(nodes, predecessor.StreamId);
            parent.DownstreamBatchSeconds = std::max(
                parent.DownstreamBatchSeconds,
                std::min(maxSeconds, predecessor.BatchSeconds + node.DownstreamBatchSeconds));

            if (--parent.Pending == 0) {
                ready.push_back(predecessor.StreamId);
            }
        }
    }

    YT_VERIFY(result->Streams.size() == nodes.size());

    return result;
}

bool IsAutomaticBufferDurationEnabled(
    const TDynamicBufferStateManagerSpec::TOneSideBufferSpecPtr& spec)
{
    return spec->MinDuration.value_or(spec->MaxDuration) < spec->MaxDuration;
}

double ComputeBufferDurationSeconds(
    const TDynamicBufferStateManagerSpec::TOneSideBufferSpecPtr& spec,
    const TStreamBufferDurationEstimate* estimate)
{
    const double cap = spec->MaxDuration.SecondsFloat();
    if (!IsAutomaticBufferDurationEnabled(spec) || !estimate) {
        return cap;
    }

    // Compare before multiplication so even an extreme finite multiplier cannot overflow.
    const double multiplier = spec->DurationMultiplier;
    const double scaled = estimate->ChainSeconds >= cap / multiplier
        ? cap
        : estimate->ChainSeconds * multiplier;
    return std::min(cap, std::max(spec->MinDuration.value_or(spec->MaxDuration).SecondsFloat(), scaled));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
